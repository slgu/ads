package util;

import java.util.UUID;

/**
 * Created by slgu1 on 11/14/15.
 */
public class Util {
    public static String uuid() {
        return UUID.randomUUID().toString();
    }
    public static String eraseGarble (byte [] thedigest) {
        StringBuilder md5StrBuff = new StringBuilder();
        for (int i = 0; i < thedigest.length; i++) {
            if (Integer.toHexString(0xFF & thedigest[i]).length() == 1) {
                md5StrBuff.append("0").append(
                        Integer.toHexString(0xFF & thedigest[i]));
            } else {
                md5StrBuff.append(Integer.toHexString(0xFF & thedigest[i]));
            }
        }
        return md5StrBuff.toString();
    }
}
