package org.tron.plugins;

import java.io.File;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.Callable;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.rocksdb.RocksDB;
import org.tron.core.capsule.AccountCapsule;
import org.tron.plugins.utils.DBUtils;
import picocli.CommandLine;


@Slf4j(topic = "filter")
@CommandLine.Command(name = "filter",
        description = "Filter account to log.",
        exitCodeListHeading = "Exit Codes:%n",
        exitCodeList = {
                "0:Successful",
                "n:Internal error: exception occurred,please check toolkit.log"})
public class DbFilter implements Callable<Integer> {

    static {
        RocksDB.loadLibrary();
    }

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;
    @CommandLine.Parameters(index = "0", defaultValue = "output-directory/database",
            description = " Input path for mainnet. Default: ${DEFAULT-VALUE}")
    private File src;
    @CommandLine.Parameters(index = "1", defaultValue = "output-directory-dst/database",
            description = "Input path for test. Default: ${DEFAULT-VALUE}")
    private File dest;

    @CommandLine.Option(names = {"--safe"},
            description = "In safe mode, read data from leveldb then put rocksdb."
                    + "If not, just change engine.properties from leveldb to rocksdb,"
                    + "rocksdb is compatible with leveldb for current version."
                    + "This may not be the case in the future."
                    + "Default: ${DEFAULT-VALUE}")
    private boolean safe;

    @CommandLine.Option(names = {"-h", "--help"})
    private boolean help;

    private static final String ACCOUNT_STORE_NAME = "account";

    @Override
    public Integer call() throws Exception {
        if (help) {
            spec.commandLine().usage(System.out);
            return 0;
        }
        if (!src.exists()) {
            logger.info(" {} does not exist.", src);
            spec.commandLine().getErr().println(spec.commandLine().getColorScheme()
                    .errorText(String.format("%s does not exist.", src)));
            return 404;
        }
        if (!dest.exists()) {
            logger.info(" {} does not exist.", dest);
            spec.commandLine().getErr().println(spec.commandLine().getColorScheme()
                    .errorText(String.format("%s does not exist.", dest)));
            return 404;
        }
        long time = System.currentTimeMillis();
        doFilter(src, dest);

        long during = (System.currentTimeMillis() - time) / 1000;
        spec.commandLine().getOut().format("convert db done, take %d s.", during).println();
        logger.info("database convert use {} seconds total.", during);
        return 0;
    }

    @SneakyThrows
    private void doFilter(File src, File dest) {
        try (DB mAccountDb = DBUtils.newLevelDb(Paths.get(src.getPath(), ACCOUNT_STORE_NAME));
             DB tAccountDb = DBUtils.newLevelDb(Paths.get(dest.getPath(), ACCOUNT_STORE_NAME));
             DBIterator levelIterator = mAccountDb.iterator(
                     new org.iq80.leveldb.ReadOptions().fillCache(false))) {
            levelIterator.seekToFirst();

            while (levelIterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = levelIterator.next();
                byte[] key = entry.getKey();
                byte[] value = entry.getValue();
                if (tAccountDb.get(key) == null) {
                    continue;
                }
                checkTrx(key,mAccountDb,true);
                checkTrx(key,tAccountDb,false);
            }
        }
    }

    private void checkTrx(byte[] key, DB mAccountDb,Boolean isMainNet) {
        AccountCapsule accountCapsule = new AccountCapsule(mAccountDb.get(key));
        long trx = accountCapsule.getTrxFrozen()+accountCapsule.getBalance()+accountCapsule.getAllowance();
        if(trx>1000000*830){
            logger.info("Big balance found account:"+ Hex.toHexString(key)+",trx:"+trx+",isMainNet:"+isMainNet);
        }
    }

}
