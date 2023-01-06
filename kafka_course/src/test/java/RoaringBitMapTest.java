import org.roaringbitmap.RoaringBitmap;

/**
 * @author zengwang
 * @create 2022-12-16 20:45
 * @desc:
 */

public class RoaringBitMapTest {
    public static void main(String[] args) {
        // 下标不能存负数
        // RoaringBitmap bitmap = RoaringBitmap.bitmapOf(1, 3, 5, -3);
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(1, 3, 5);
        System.out.println(bitmap);
        // 输出bitmap中1的个数(元素个数)
        System.out.println(bitmap.getCardinality());

        // 判断一共元素是否存在
        System.out.println(bitmap.contains(7));
        bitmap.add(8);

        RoaringBitmap bitmap1 = RoaringBitmap.bitmapOf(2, 3, 5);
        // 更新bitmap自身 按位或
        bitmap.or(bitmap1);
        System.out.println(bitmap);
    }
}
