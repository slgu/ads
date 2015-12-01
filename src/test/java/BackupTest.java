import com.sun.management.OperatingSystemMXBean;
import config.Config;
import dedup.FileHashing;
import dedup.VaribleLengthHashing;
import operation.FileOp;

import javax.management.MBeanServerConnection;
import java.io.*;
import java.lang.management.ManagementFactory;

/**
 * Created by slgu1 on 11/30/15.
 */
public class BackupTest {
    private static String imgDir = "/Users/slgu1/ads_data/BackupVm";
    public static void put() throws IOException {
        File dir = new File(imgDir);
        for (File f: dir.listFiles()) {
            if (f.isFile()) {
                if (f.getName().startsWith("."))
                    continue;
                InputStream io = null;
                try {
                    io = new BufferedInputStream(new FileInputStream(f.getAbsoluteFile()));
                }
                catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
                System.out.println("begin add:" + f.getName());
                boolean res = FileOp.create(f.getName(), io, f.getAbsolutePath());
                System.out.println(res?"succeed":"fail");
                io.close();
            }
        }
    }
    public static void fileLevelTest() throws IOException{
        System.out.println("begin file level test");
        Config.dedup = new FileHashing();
        put();
    }
    public static void VariableTest() throws IOException {
        System.out.println("begin variable size test");
        try {
            long msk = 0xFFFFFFFFL;
            System.out.println(msk);
            Config.dedup = new VaribleLengthHashing(msk);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        put();
    }
    public static void main(String [] args) throws IOException{
        MBeanServerConnection mbsc = ManagementFactory.getPlatformMBeanServer();
        OperatingSystemMXBean osBean = ManagementFactory.newPlatformMXBeanProxy(
                mbsc, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME, OperatingSystemMXBean.class);
        long nanoBefore = System.nanoTime();
        long cpuBefore = osBean.getProcessCpuTime();
        //run test here
        VariableTest();
        long cpuAfter = osBean.getProcessCpuTime();
        long nanoAfter = System.nanoTime();
        long percent;
        if (nanoAfter > nanoBefore)
            percent = ((cpuAfter-cpuBefore)*100L)/
                    (nanoAfter-nanoBefore);
        else percent = 0;
        System.out.println("Cpu usage: " + percent + "%");
        System.out.println("Time usage: " + 1.0 * (nanoAfter - nanoBefore) / 1000000000);
    }
}
