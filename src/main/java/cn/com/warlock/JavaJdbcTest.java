package cn.com.warlock;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;

import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

public class JavaJdbcTest {

    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";

    private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://hlg-2p239-fandongsheng:3306/test";

    private static final String MYSQL_USERNAME = "root";

    private static final String MYSQL_PWD = "bfd_123";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaJdbcTest");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        DbConnection dbConnection = new DbConnection(MYSQL_DRIVER, MYSQL_CONNECTION_URL, MYSQL_USERNAME, MYSQL_PWD);

        // Load data from MySQL
        JdbcRDD<Object[]> jdbcRDD = new JdbcRDD<>(jsc.sc(), dbConnection, "select name,age from stu where age >= ? and age <= ?", 18, 19, 10, new MapResult(),
                                                  ClassManifestFactory$.MODULE$.fromClass(Object[].class));

        // Convert to JavaRDD
        JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, ClassManifestFactory$.MODULE$.fromClass(Object[].class));

        // name:age
        List<String> employeeFullNameList = javaRDD.map(new Function<Object[], String>() {
            @Override
            public String call(final Object[] record) throws Exception {
                return record[0] + ":" + record[1];
            }
        }).collect();

        for (String fullName : employeeFullNameList) {
            System.out.println(fullName);
        }

        jsc.close();
    }

    static class DbConnection extends AbstractFunction0<Connection> implements Serializable {

        private final String driverClassName;

        private final String connectionUrl;

        private final String userName;

        private final String password;

        public DbConnection(String driverClassName, String connectionUrl, String userName, String password) {
            this.driverClassName = driverClassName;
            this.connectionUrl = connectionUrl;
            this.userName = userName;
            this.password = password;
        }

        @Override
        public Connection apply() {
            try {
                Class.forName(driverClassName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            Properties properties = new Properties();
            properties.setProperty("user", userName);
            properties.setProperty("password", password);

            Connection connection = null;
            try {
                connection = DriverManager.getConnection(connectionUrl, properties);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return connection;
        }
    }

    static class MapResult extends AbstractFunction1<ResultSet, Object[]> implements Serializable {

        @Override
        public Object[] apply(ResultSet row) {
            return JdbcRDD.resultSetToObjectArray(row);
        }
    }
}