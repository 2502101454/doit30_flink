package cn.doitedu.flink.demos;

import lombok.*;

import java.util.Map;

/**
 * @author zengwang
 * @create 2023-04-22 22:17
 * @desc:
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class EventLog {

    private long guid;
    private String sessionId;
    private String eventId;
    private long timestamp;
    private Map<String, String> eventInfo;

}
