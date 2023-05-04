package cn.doitedu.flink.exercises;

/**
 * @author zengwang
 * @create 2023-05-03 21:51
 * @desc:
 */
import lombok.*;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserInfo {
    private int id;
    private String gender;
    private String city;
}