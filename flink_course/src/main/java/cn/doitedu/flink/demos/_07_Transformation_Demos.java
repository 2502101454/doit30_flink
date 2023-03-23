package cn.doitedu.flink.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.xml.Null;

import java.io.Serializable;
import java.util.List;

/**
 * @author zengwang
 * @create 2023-03-15 17:14
 * @desc:
 */
public class _07_Transformation_Demos {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> beanStream = env.readTextFile("flink_course/src/data/transformation_input/userinfo.txt");
        /**
         * map算子演示：把每条json转成javaBean对象
         */
        SingleOutputStreamOperator<UserInfo> userStream = beanStream.map(value -> JSON.parseObject(value, UserInfo.class));
        // userStream.print();

        /**
         * filter算子演示
         *  过滤好用超过3位的用户数据
         */
        SingleOutputStreamOperator<UserInfo> filter = userStream.filter(value -> value.getFriends().size() <= 3);
        // filter.print();
        /**
         * flatmap算子演示
         *  把每个用户的好友信息，全部提取出来(带上用户自身的信息)，打平，放入结果流
         *  {"uid":1,"gender":"male","name":"ua","friends":[{"fid":1,"name":"cc"},{"fid":3,"name":"bb"}]}
         *  ==>
         *  {"uid":1,"gender":"male","name":"ua","fid":1,"name":"cc"}
         *  {"uid":1,"gender":"male","name":"ua","fid":3,"name":"bb"}
         */
        SingleOutputStreamOperator<UserFriendInfo> flatJson = filter.flatMap(new FlatMapFunction<UserInfo, UserFriendInfo>() {
            @Override
            public void flatMap(UserInfo value, Collector<UserFriendInfo> out) throws Exception {
                Integer uid = value.getUid();
                String name = value.getName();
                String gender = value.getGender();

                for (FriendInfo friend : value.getFriends()) {
                    out.collect(new UserFriendInfo(uid, name, gender, friend.getFid(), friend.getName()));
                }
            }
        });
        // flatJson.print();

        /**
         * keyBy算子演示
         *   对上一步的结果，按用户性别分组分组
         *
         *   滚动聚合算子(只能在KeyedStream 流上调用):  sum、min、minBy、max、maxBy、reduce算子演示
         *
         *   共统计:
         *      各个性别用户的好友总数；
         *
         *      各个性别用户的好友数量最大值； (max就可以了)
         *      各个性别用户的好友数量最大的那个人；(maxBy)
         *
         *      各个性别用户的好友数量最小值；
         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> genderOne = flatJson.map(value ->
                Tuple2.of(value.getGender(), 1)).returns(new TypeHint<Tuple2<String, Integer>>() {});

        // 各个性别用户的好友总数
       /* genderOne.keyBy(tp -> tp.f0)
                        .sum("f1")
                                .print();*/

        // 各个性别用户的好友数量最大
        /**
         * max：开始，首条数据为最大记录，输出
         *           当随后更大值的出现时，就会只更新求最大值的字段到最大记录，输出新的最大记录;
         *           随后没有更大的值出现时，继续输出上次的最大记录;
         *
         * maxBy: 开始，首条数据为最大记录，输出
         *          当随后更大值的出现时，最大记录更新为更大值的那条记录，输出新的最大记录;
         *          随后没有更大的值出现时，继续输出上次的最大记录;
         *
         *  总结：
         *      max 首条 + 局部字段更新
         *      maxBy 整条更新
          */

        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> tuple4 = userStream.map(value ->
                        Tuple4.of(value.getUid(), value.getGender(), value.getName(), value.getFriends().size()))
                        .returns(new TypeHint<Tuple4<Integer, String, String, Integer>>() {});

        tuple4.keyBy(tp -> tp.f1)
                .minBy("f3");
                // .print();

        /**
         * reduce 算子演示：实现各个性别，求好友数最大，且当前后两条数据的好友数一样时，输出后来的这条数据
         *
         * value1: 此前的状态(聚合值)，初始时为null
         * value2：本次新到数据
         * 返回：更新后的状态
         */
        tuple4.keyBy(tp -> tp.f1).reduce(new ReduceFunction<Tuple4<Integer, String, String, Integer>>() {
            @Override
            public Tuple4<Integer, String, String, Integer> reduce(Tuple4<Integer, String, String, Integer> value1,
                                                                   Tuple4<Integer, String, String, Integer> value2) throws Exception {
                if (value1 == null || value2.f3 >= value1.f3) {
                    return value2;
                }
                return value1;
            }
        }); //.print();

        /**
         * reduce算子演示： 实现sum，输出后到的数据
         */
        tuple4.keyBy(tp -> tp.f1).reduce(new ReduceFunction<Tuple4<Integer, String, String, Integer>>(){
            @Override
            public Tuple4<Integer, String, String, Integer> reduce(Tuple4<Integer, String, String, Integer> value1,
                                                                   Tuple4<Integer, String, String, Integer> value2) throws Exception {
                value2.setField(value2.f3 + (value1.f3 == null ? 0: value1.f3), 3);

                return value2;
            }
        }).print();
        // min 和minBy 同理
        env.execute();

    }
}


@Data
class UserInfo implements Serializable {
    private int uid;
    private String name;
    private String gender;
    private List<FriendInfo> friends;
}

@Data
class FriendInfo implements Serializable {
    private int fid;
    private String name;
}

@Data
@AllArgsConstructor
class UserFriendInfo implements Serializable {
    private int uid;
    private String name;
    private String gender;
    private int fid;
    private String fname;
}