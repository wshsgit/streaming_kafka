import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.List;
import java.util.Map;

public class BufferedRandomAccessFileTest {

    /**
     * 通过BufferedRandomAccessFile读取文件,推荐
     *
     * @param file     源文件
     * @param encoding 文件编码
     * @param pos      偏移量
     * @param num      读取量
     * @return pins文件内容，pos当前偏移量
     */
    public static Map<String, Object> BufferedRandomAccessFileReadLine(File file, String encoding, long pos, int num) {

        Map<String, Object> res = Maps.newHashMap();
        List<String> pins = Lists.newArrayList();
        res.put("pins", pins);
        BufferedRandomAccessFile reader = null;

        try {
            reader = new BufferedRandomAccessFile(file, "r");
            reader.seek(pos);

            for (int i = 0; i < num; i++) {
                String pin = reader.readLine();
                if (StringUtils.isBlank(pin)) {
                    break;
                }
                pins.add(new String(pin.getBytes("8859_1"), encoding));
            }

            res.put("pos", reader.getFilePointer());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(reader);
        }

        return res;
    }

}
