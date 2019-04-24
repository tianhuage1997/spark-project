package cn.tmooc.spark;

import cn.tmooc.conf.ConfigurationManager;
import cn.tmooc.constant.Constants;
import cn.tmooc.dao.DAOFactory;
import cn.tmooc.dao.ITaskDAO;
import cn.tmooc.domain.Task;
import cn.tmooc.test.MockData;
import cn.tmooc.util.ParamUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * 用户访问session分析spark作业
 *
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 *
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男/女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 *
 * 我们的spark作业如何接受用户创建的任务？
 *
 * J2EE平台在接受用户创建任务的请求之后，会将任务信息插入mysql的task表中，任务参数以JSON格式封装在task_param
 * 字段中
 *
 * 接着J2EE平台会执行我们的spark-submit  shell脚本，并将taskid作为参数传递给spark-submit  shell脚本
 * spark-submit  shell脚本，在执行时，是可以接受参数的，并且会将接收的参数，传递给spark作业的main函数
 *
 * 这是spark本身提供的特性
 */
public class UservisitSessionAnalyzeSpark {

    public static void main(String[] args) {

        //构建spark上下文
        SparkConf sparkConf=new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster(Constants.SPARK_APP_NAME_Master);

        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        SQLContext sqlContext=getSQLContext(sc.sc());

        //生成模拟测试数据
        mockData(sc,sqlContext);
        //创建需要使用的dao组件
        ITaskDAO  taskDAO= DAOFactory.getTaskDAO();

        //那么就首先得查询出来指定的任务,并获取任务的查询参数
        long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task task =taskDAO.findById(taskid);
        //把task对象里面的taskParam里面的所有json数据取出来
        JSONObject taskParam=JSONObject.parseObject(task.getTaskParam());
        //如果要进行session粒度的数据聚合
        //首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row>  actionRDD=getActionRDDByDateRange(sqlContext,taskParam);

        //首先，可以将行为数据，按照session_id进行groupByKey分组
        //此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
        //与用户信息数据，进行json
        //然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user信息
        JavaPairRDD<String,String> sessionid2AggrInfoRDD= aggregateBySession(sqlContext,actionRDD);



        //如果要根据用户在创建任务时指定的参数，来进行数据过滤和筛选



        //关闭spaek上下文
        sc.close();

    }

    /**
     * 获取SQLcontext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext
     * @param sc  SparkContext
     * @return
     */
    private static SQLContext  getSQLContext(SparkContext sc){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
             return  new SQLContext(sc);
        }else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     * @param sc
     * @param sqlContext
     */
    private  static  void mockData(JavaSparkContext sc,SQLContext sqlContext){
            boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
            if(local){
                MockData.mock(sc,sqlContext);
            }
    }

    /**
     *获取指定日期范围内的用户行为数据
     * @param sqlContext SQLContext
     * @param taskParam   任务参数
     * @return  行为数据RDD
     */
    private  static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext,JSONObject taskParam){
            String startDate=ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
            String endDate=ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);

            String sql= "select * from user_visit_action where date >= '"+startDate+"' and date <= '"+endDate+"'";




    }
}
