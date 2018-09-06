package org.tron.core.db;


import static org.tron.protos.Protocol.Transaction.Contract.ContractType.TransferAssetContract;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.utils.ByteArray;
import org.tron.core.Constant;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.AssetIssueCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionResultCapsule;
import org.tron.core.exception.AccountResourceInsufficientException;
import org.tron.core.exception.ContractValidateException;
import org.tron.core.exception.TooBigTransactionResultException;
import org.tron.protos.Contract.TransferAssetContract;
import org.tron.protos.Contract.TransferContract;
import org.tron.protos.Protocol.Transaction.Contract;

@Slf4j
public class BandwidthProcessor extends ResourceProcessor {

  public BandwidthProcessor(Manager manager) {
    super(manager);
  }

  @Override
  public void updateUsage(AccountCapsule accountCapsule) {
    long now = dbManager.getWitnessController().getHeadSlot();
    updateUsage(accountCapsule, now);
  }

  private void updateUsage(AccountCapsule accountCapsule, long now) {
    long oldNetUsage = accountCapsule.getNetUsage();
    long latestConsumeTime = accountCapsule.getLatestConsumeTime();
    accountCapsule.setNetUsage(increase(oldNetUsage, 0, latestConsumeTime, now));
    long oldFreeNetUsage = accountCapsule.getFreeNetUsage();
    long latestConsumeFreeTime = accountCapsule.getLatestConsumeFreeTime();
    accountCapsule.setFreeNetUsage(increase(oldFreeNetUsage, 0, latestConsumeFreeTime, now));
    Map<String, Long> assetMap = accountCapsule.getAssetMap();
    assetMap.forEach((assetName, balance) -> {
      long oldFreeAssetNetUsage = accountCapsule.getFreeAssetNetUsage(assetName);
      long latestAssetOperationTime = accountCapsule.getLatestAssetOperationTime(assetName);
      accountCapsule.putFreeAssetNetUsage(assetName,
          increase(oldFreeAssetNetUsage, 0, latestAssetOperationTime, now));
    });
  }

  @Override
  public void consume(TransactionCapsule trx, TransactionResultCapsule ret,
      TransactionTrace trace)
      throws ContractValidateException, AccountResourceInsufficientException, TooBigTransactionResultException {
    List<Contract> contracts = trx.getInstance().getRawData().getContractList();
    if (trx.getResultSerializedSize() > Constant.MAX_RESULT_SIZE_IN_TX * contracts.size()) {
      throw new TooBigTransactionResultException();
    }

    long bytesSize;
    if (dbManager.getDynamicPropertiesStore().supportVM()) {
      TransactionCapsule txCapForEstimateBandWidth = new TransactionCapsule(
          trx.getInstance().getRawData(),
          trx.getInstance().getSignatureList());
      bytesSize = txCapForEstimateBandWidth.getSerializedSize();
    } else {
      bytesSize = trx.getSerializedSize();
    }

    for (Contract contract : contracts) {
      if (dbManager.getDynamicPropertiesStore().supportVM()) {
        bytesSize += Constant.MAX_RESULT_SIZE_IN_TX;
      }

      logger.debug("trxId {},bandwidth cost :{}", trx.getTransactionId(), bytesSize);
      trace.setNetBill(bytesSize, 0);
      byte[] address = TransactionCapsule.getOwner(contract);
      AccountCapsule accountCapsule = dbManager.getAccountStore().get(address);
      if (accountCapsule == null) {
        throw new ContractValidateException("account not exists");
      }
      long now = dbManager.getWitnessController().getHeadSlot();

      // 如果需要创建账户，则创建账户，返回true；否则返回false
      if (contractCreateNewAccount(contract)) {
        // 如果是转账类型，TRX或其他Token，仅消耗创建账户合约的BP，转账合约的BP不再消耗了
        consumeForCreateNewAccount(accountCapsule, bytesSize, now, ret);
        trace.setNetBill(0, ret.getFee());
        continue;
      }

      if (contract.getType() == TransferAssetContract) {
        // 转账发布的Token，优先扣除发布Token搜有者的BP
        if (useAssetAccountNet(contract, accountCapsule, now, bytesSize)) {
          continue;
        }
      }

      // 扣除调用者 通过冻结TRX获取的 BP
      if (useAccountNet(accountCapsule, bytesSize, now)) {
        continue;
      }

      // 扣除调用者免费的BP
      if (useFreeNet(accountCapsule, bytesSize, now)) {
        continue;
      }

      // 扣费 10sun/byte
      if (useTransactionFee(accountCapsule, bytesSize, ret)) {
        trace.setNetBill(0, ret.getFee());
        continue;
      }

      long fee = dbManager.getDynamicPropertiesStore().getTransactionFee() * bytesSize;
      throw new AccountResourceInsufficientException(
          "Account Insufficient bandwidth[" + bytesSize + "] and balance["
              + fee + "] to create new account");
    }
  }

  private boolean useTransactionFee(AccountCapsule accountCapsule, long bytes,
      TransactionResultCapsule ret) {
    long fee = dbManager.getDynamicPropertiesStore().getTransactionFee() * bytes;
    if (consumeFee(accountCapsule, fee)) {
      ret.addFee(fee);
      dbManager.getDynamicPropertiesStore().addTotalTransactionCost(fee);
      return true;
    } else {
      return false;
    }
  }

  // 扣除创建账户消耗的BP 或 费用
  private void consumeForCreateNewAccount(AccountCapsule accountCapsule, long bytes,
      long now, TransactionResultCapsule resultCapsule)
      throws AccountResourceInsufficientException {
    boolean ret = consumeBandwidthForCreateNewAccount(accountCapsule, bytes, now);

    if (!ret) {
      // 如果 通过冻结TRX获取的BP 不足以支付，则扣除0.1TRX代替
      ret = consumeFeeForCreateNewAccount(accountCapsule, resultCapsule);
      if (!ret) {
        throw new AccountResourceInsufficientException();
      }
    }
  }

  // 如果用户的BP足够支付，则扣除消耗的BP，返回true；否则返回false
  public boolean consumeBandwidthForCreateNewAccount(AccountCapsule accountCapsule, long bytes,
      long now) {

    long createNewAccountBandwidthRatio = dbManager.getDynamicPropertiesStore()
        .getCreateNewAccountBandwidthRate();

    long netUsage = accountCapsule.getNetUsage();
    long latestConsumeTime = accountCapsule.getLatestConsumeTime();
    long netLimit = calculateGlobalNetLimit(accountCapsule.getFrozenBalance());

    long newNetUsage = increase(netUsage, 0, latestConsumeTime, now);

    if (bytes * createNewAccountBandwidthRatio <= (netLimit - newNetUsage)) {
      latestConsumeTime = now;
      long latestOperationTime = dbManager.getHeadBlockTimeStamp();
      newNetUsage = increase(newNetUsage, bytes * createNewAccountBandwidthRatio, latestConsumeTime,
          now);
      accountCapsule.setLatestConsumeTime(latestConsumeTime);
      accountCapsule.setLatestOperationTime(latestOperationTime);
      accountCapsule.setNetUsage(newNetUsage);
      dbManager.getAccountStore().put(accountCapsule.createDbKey(), accountCapsule);
      return true;
    }
    return false;
  }

  public boolean consumeFeeForCreateNewAccount(AccountCapsule accountCapsule,
      TransactionResultCapsule ret) {
    long fee = dbManager.getDynamicPropertiesStore().getCreateAccountFee();
    if (consumeFee(accountCapsule, fee)) {
      ret.addFee(fee);
      dbManager.getDynamicPropertiesStore().addTotalCreateAccountCost(fee);
      return true;
    } else {
      return false;
    }
  }

  public boolean contractCreateNewAccount(Contract contract) {
    AccountCapsule toAccount;
    switch (contract.getType()) {
      case AccountCreateContract:
        return true;
      case TransferContract:
        TransferContract transferContract;
        try {
          transferContract = contract.getParameter().unpack(TransferContract.class);
        } catch (Exception ex) {
          throw new RuntimeException(ex.getMessage());
        }
        toAccount = dbManager.getAccountStore().get(transferContract.getToAddress().toByteArray());
        return toAccount == null;
      case TransferAssetContract:
        TransferAssetContract transferAssetContract;
        try {
          transferAssetContract = contract.getParameter().unpack(TransferAssetContract.class);
        } catch (Exception ex) {
          throw new RuntimeException(ex.getMessage());
        }
        toAccount = dbManager.getAccountStore()
            .get(transferAssetContract.getToAddress().toByteArray());
        return toAccount == null;
      default:
        return false;
    }
  }


  // 优先扣除发布Token所有者 的BP，都满足扣除，则返回true；否则返回false
  private boolean useAssetAccountNet(Contract contract, AccountCapsule accountCapsule, long now,
      long bytes)
      throws ContractValidateException {

    ByteString assetName;
    try {
      assetName = contract.getParameter().unpack(TransferAssetContract.class).getAssetName();
    } catch (Exception ex) {
      throw new RuntimeException(ex.getMessage());
    }
    String assetNameString = ByteArray.toStr(assetName.toByteArray());
    AssetIssueCapsule assetIssueCapsule
        = dbManager.getAssetIssueStore().get(assetName.toByteArray());
    if (assetIssueCapsule == null) {
      throw new ContractValidateException("asset not exists");
    }

    if (assetIssueCapsule.getOwnerAddress() == accountCapsule.getAddress()) {
      return useAccountNet(accountCapsule, bytes, now);
    }

    long publicFreeAssetNetLimit = assetIssueCapsule.getPublicFreeAssetNetLimit();
    long publicFreeAssetNetUsage = assetIssueCapsule.getPublicFreeAssetNetUsage();
    long publicLatestFreeNetTime = assetIssueCapsule.getPublicLatestFreeNetTime();

    long newPublicFreeAssetNetUsage = increase(publicFreeAssetNetUsage, 0,
        publicLatestFreeNetTime, now);

    // 判断 发布Token 所有调用者共用BP 剩余量是否足够
    if (bytes > (publicFreeAssetNetLimit - newPublicFreeAssetNetUsage)) {
      logger.debug("The " + assetNameString + " public free bandwidth is not enough");
      return false;
    }

    long freeAssetNetLimit = assetIssueCapsule.getFreeAssetNetLimit();

    long freeAssetNetUsage = accountCapsule
        .getFreeAssetNetUsage(assetNameString);
    long latestAssetOperationTime = accountCapsule
        .getLatestAssetOperationTime(assetNameString);

    long newFreeAssetNetUsage = increase(freeAssetNetUsage, 0,
        latestAssetOperationTime, now);

    // 判断 调用者 发布资产使用BP 剩余量是否足够扣除
    if (bytes > (freeAssetNetLimit - newFreeAssetNetUsage)) {
      logger.debug("The " + assetNameString + " free bandwidth is not enough");
      return false;
    }

    AccountCapsule issuerAccountCapsule = dbManager.getAccountStore()
        .get(assetIssueCapsule.getOwnerAddress().toByteArray());

    long issuerNetUsage = issuerAccountCapsule.getNetUsage();
    long latestConsumeTime = issuerAccountCapsule.getLatestConsumeTime();
    long issuerNetLimit = calculateGlobalNetLimit(issuerAccountCapsule.getFrozenBalance());

    long newIssuerNetUsage = increase(issuerNetUsage, 0, latestConsumeTime, now);

    // 发布Token 所有者 冻结TRX获取BP 剩余量是否满足
    if (bytes > (issuerNetLimit - newIssuerNetUsage)) {
      logger.debug("The " + assetNameString + " issuer'bandwidth is not enough");
      return false;
    }

    latestConsumeTime = now;
    latestAssetOperationTime = now;
    publicLatestFreeNetTime = now;
    long latestOperationTime = dbManager.getHeadBlockTimeStamp();
    newIssuerNetUsage = increase(newIssuerNetUsage, bytes, latestConsumeTime, now);
    newFreeAssetNetUsage = increase(newFreeAssetNetUsage,
        bytes, latestAssetOperationTime, now);
    newPublicFreeAssetNetUsage = increase(newPublicFreeAssetNetUsage, bytes,
        publicLatestFreeNetTime, now);

    issuerAccountCapsule.setNetUsage(newIssuerNetUsage);
    issuerAccountCapsule.setLatestConsumeTime(latestConsumeTime);

    accountCapsule.setLatestOperationTime(latestOperationTime);
    accountCapsule.putLatestAssetOperationTimeMap(assetNameString,
        latestAssetOperationTime);
    accountCapsule.putFreeAssetNetUsage(assetNameString, newFreeAssetNetUsage);

    assetIssueCapsule.setPublicFreeAssetNetUsage(newPublicFreeAssetNetUsage);
    assetIssueCapsule.setPublicLatestFreeNetTime(publicLatestFreeNetTime);

    dbManager.getAccountStore().put(accountCapsule.createDbKey(), accountCapsule);
    dbManager.getAccountStore().put(issuerAccountCapsule.createDbKey(),
        issuerAccountCapsule);
    dbManager.getAssetIssueStore().put(assetIssueCapsule.createDbKey(), assetIssueCapsule);

    return true;

  }

  public long calculateGlobalNetLimit(long frozeBalance) {
    if (frozeBalance < 1000_000L) {
      return 0;
    }
    long netWeight = frozeBalance / 1000_000L;
    long totalNetLimit = dbManager.getDynamicPropertiesStore().getTotalNetLimit();
    long totalNetWeight = dbManager.getDynamicPropertiesStore().getTotalNetWeight();
    assert totalNetWeight > 0;
    return (long) (netWeight * ((double) totalNetLimit / totalNetWeight));
  }

  private boolean useAccountNet(AccountCapsule accountCapsule, long bytes, long now) {

    long netUsage = accountCapsule.getNetUsage();
    long latestConsumeTime = accountCapsule.getLatestConsumeTime();
    long netLimit = calculateGlobalNetLimit(accountCapsule.getFrozenBalance());

    long newNetUsage = increase(netUsage, 0, latestConsumeTime, now);

    if (bytes > (netLimit - newNetUsage)) {
      logger.debug("net usage is running out. now use free net usage");
      return false;
    }

    latestConsumeTime = now;
    long latestOperationTime = dbManager.getHeadBlockTimeStamp();
    newNetUsage = increase(newNetUsage, bytes, latestConsumeTime, now);
    accountCapsule.setNetUsage(newNetUsage);
    accountCapsule.setLatestOperationTime(latestOperationTime);
    accountCapsule.setLatestConsumeTime(latestConsumeTime);

    dbManager.getAccountStore().put(accountCapsule.createDbKey(), accountCapsule);
    return true;
  }

  private boolean useFreeNet(AccountCapsule accountCapsule, long bytes, long now) {

    long freeNetLimit = dbManager.getDynamicPropertiesStore().getFreeNetLimit();
    long freeNetUsage = accountCapsule.getFreeNetUsage();
    long latestConsumeFreeTime = accountCapsule.getLatestConsumeFreeTime();
    long newFreeNetUsage = increase(freeNetUsage, 0, latestConsumeFreeTime, now);

    if (bytes > (freeNetLimit - newFreeNetUsage)) {
      logger.debug("free net usage is running out");
      return false;
    }

    long publicNetLimit = dbManager.getDynamicPropertiesStore().getPublicNetLimit();
    long publicNetUsage = dbManager.getDynamicPropertiesStore().getPublicNetUsage();
    long publicNetTime = dbManager.getDynamicPropertiesStore().getPublicNetTime();

    long newPublicNetUsage = increase(publicNetUsage, 0, publicNetTime, now);

    if (bytes > (publicNetLimit - newPublicNetUsage)) {
      logger.debug("free public net usage is running out");
      return false;
    }

    latestConsumeFreeTime = now;
    long latestOperationTime = dbManager.getHeadBlockTimeStamp();
    publicNetTime = now;
    newFreeNetUsage = increase(newFreeNetUsage, bytes, latestConsumeFreeTime, now);
    newPublicNetUsage = increase(newPublicNetUsage, bytes, publicNetTime, now);
    accountCapsule.setFreeNetUsage(newFreeNetUsage);
    accountCapsule.setLatestConsumeFreeTime(latestConsumeFreeTime);
    accountCapsule.setLatestOperationTime(latestOperationTime);

    dbManager.getDynamicPropertiesStore().savePublicNetUsage(newPublicNetUsage);
    dbManager.getDynamicPropertiesStore().savePublicNetTime(publicNetTime);
    dbManager.getAccountStore().put(accountCapsule.createDbKey(), accountCapsule);
    return true;

  }

}


