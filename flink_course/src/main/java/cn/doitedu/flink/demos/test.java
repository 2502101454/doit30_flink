package cn.doitedu.flink.demos;

import java.text.MessageFormat;

/**
 * @author zengwang
 * @create 2023-05-01 11:47
 * @desc:
 */
public class test {
    public static void main(String[] args) {
        String wangzne = MessageFormat.format("name {0} age {1} city {2}", "wangzne", 23, null);
        System.out.println(wangzne);
        System.out.println(System.currentTimeMillis());
    }
}
