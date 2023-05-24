package cn.doitedu.flink.demos;

/**
 * @author zengwang
 * @create 2023-05-24 12:42
 * @desc:
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventBean2 {
    private long guid;
    private String eventId;
    private long timestamp;
    private String pageId;
    private int actTimeLong;  // 行为时长
}