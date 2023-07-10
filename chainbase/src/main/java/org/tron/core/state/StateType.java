package org.tron.core.state;

import com.google.common.primitives.Bytes;
import java.util.Arrays;
import lombok.Getter;

public enum StateType {

  //1.先测试不压缩
  // 写入trie   mdb ,       索引写入(搞索引结构)，
  // 21年交易：litenode 写trie 预计一天30g
  // mdb kv 几次put 目标熟悉erigon 写入逻辑，业务构造写入，需要多久 （和erigon 一致）
  // changeset 方式写入
  // 可能影响：erigon自身针对字段压缩，java go版本mdb差异(数据占用对比)，
  // java调用go 还是直接使用java版，java版对比实际存储差异，java go 按接口 采用erigon db测试方案，lmdb-java方案
  // 数据跑多久，机器配置？几台机器？
  // type保留 account,account-asset,delegation,storage-row

  UNDEFINED((byte) 0x00, "undefined"),

  Account((byte) 0x01, "account"),//去掉asset
  AccountAsset((byte) 0x02, "account-asset"),
  AccountIndex((byte) 0x03, "account-index"),
  AccountIdIndex((byte) 0x04, "accountid-index"),
  AssetIssue((byte) 0x05, "asset-issue-v2"),
  Code((byte) 0x07, "code"),
  Contract((byte) 0x08, "contract"),
  Delegation((byte) 0x09, "delegation"),
  DelegatedResource((byte) 0x0a, "DelegatedResource"),
  DelegatedResourceAccountIndex((byte) 0x0b, "DelegatedResourceAccountIndex"),
  Exchange((byte) 0x0c, "exchange"),
  ExchangeV2((byte) 0x0d, "exchange-v2"),
  IncrementalMerkleTree((byte) 0x0e, "IncrementalMerkleTree"),
  MarketAccount((byte) 0x0f, "market_account"),
  MarketOrder((byte) 0x10, "market_order"),
  MarketPairPriceToOrder((byte) 0x11, "market_pair_price_to_order"),
  MarketPairToPrice((byte) 0x12, "market_pair_to_price"),
  Nullifier((byte) 0x13, "nullifier"),
  Properties((byte) 0x14, "properties"),
  Proposal((byte) 0x15, "proposal"),
  StorageRow((byte) 0x16, "storage-row"),
  Votes((byte) 0x17, "votes"),
  Witness((byte) 0x18, "witness"),
  WitnessSchedule((byte) 0x19, "witness_schedule"),
  ContractState((byte) 0x20, "contract-state");


  private final byte value;
  @Getter
  private final String name;

  StateType(byte value, String name) {
    this.value = value;
    this.name = name;
  }

  public byte value() {
    return this.value;
  }

  public static StateType get(String name) {
    return Arrays.stream(StateType.values()).filter(type -> type.name.equals(name))
        .findFirst().orElse(UNDEFINED);
  }

  public static StateType get(byte value) {
    return Arrays.stream(StateType.values()).filter(type -> type.value == value)
        .findFirst().orElse(UNDEFINED);
  }

  public static byte[] encodeKey(StateType type, byte[] key) {
    byte[] p = new byte[]{type.value};
    return Bytes.concat(p, key);
  }

  public static byte[] decodeKey(byte[] key) {
    return Arrays.copyOfRange(key, 1, key.length);
  }

}
