package org.apache.hadoop.examples;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

 
public class focus_analysis {
	static Map<String,String> focus_map;
    private static class MysqlRecord implements DBWritable,Writable {
        protected String movie_id ;
        protected String content ;
        protected String  score ;
 
        @Override
        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setString(1,this.movie_id);
            preparedStatement.setString(2, content);
            preparedStatement.setString(3,this.score);
        }
 
        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            this.movie_id = resultSet.getString(1) ;
            this.content = resultSet.getString(2);
            this.score = resultSet.getString(3) ;
        }
 
        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(movie_id);
            dataOutput.writeUTF(content);
//            Text.writeString(dataOutput, this.time.toString()) ;
            dataOutput.writeUTF(score);
        }
 
        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.movie_id = dataInput.readUTF() ;
            this.content =dataInput.readUTF();
            this.score = dataInput.readUTF();
        }
 
        @Override
        public String toString() {
            return String.format("%d\t%s\t%d\t%s",movie_id,content,score);
        }
    }

    private static class SQLMapper extends Mapper<Object, MysqlRecord, Text, Text> {
    	
    	 public static Map focus_map(String path) throws IOException{
    		 Map<String,String> focus_map = new HashMap<String,String>();
    		 File file = new File(path);
    	     File[] dirlist = file.listFiles();
    	     for (File dir : dirlist)
    	     {
    	         if (dir.isDirectory()) 
    	         {
    	        	 File[] files = dir.listFiles();
    	        	 for(File f : files)
    	        	 {
    	        		 BufferedReader br = new BufferedReader(new FileReader(f));//构造一个BufferedReader类来读取文件
    	                 String s = null;
    	                 String focus = f.getName().replaceAll(".txt", "");
    	                 System.out.println(focus);
    	                 while((s = br.readLine())!=null)
    	                 {//使用readLine方法，一次读一行
    	                	 focus_map.put(s.replaceAll("\n", ""), focus);
    	                 }
    	                 br.close();   
    	        	 }
    	         } 
    	     }
    	     for(String key:focus_map.keySet()){
    	         System.out.println(key+"/"+focus_map.get(key));
    	     }
    		return focus_map;
    	 }
    	 @Override
         protected void setup(Context context)
                 throws IOException, InterruptedException {

    	    	 focus_map = focus_map("/home/hadoopnew/下载/FouceAnalysis-master/关注点标注4/");
                 //从全局配置获取配置参数

                
                
             }
        @Override
        protected void map(Object key, MysqlRecord value, Context context)
                throws IOException, InterruptedException {
        	
        	
            String content = value.content ;
            String movie_id = value.movie_id;
            String score = value.score;
            content = content.replaceAll("\\s|\n", "");
            for(String mkey:focus_map.keySet())
            {
 
                if (content.indexOf(mkey)!=-1)
                {

//                	System.out.println(content+'/'+mkey+"/"+focus_map.get(mkey));
                	context.write(new Text(movie_id),new Text(focus_map.get(mkey)));
                }
                	
            }


           
        }
    }
    public static String sortMap(Map<String, Integer> map,double sum){
        //获取entrySet
        Set<Map.Entry<String,Integer>> mapEntries = map.entrySet();
        
        for(Entry<String, Integer> entry : mapEntries){
            System.out.println("key:" +entry.getKey()+"   value:"+entry.getValue() );
        }
        
        //使用链表来对集合进行排序，使用LinkedList，利于插入元素
        List<Map.Entry<String, Integer>> result = new LinkedList<>(mapEntries);
        //自定义比较器来比较链表中的元素
        Collections.sort(result, new Comparator<Entry<String, Integer>>() {
            //基于entry的值（Entry.getValue()），来排序链表
            @Override
            public int compare(Entry<String, Integer> o1,
                    Entry<String, Integer> o2) {
                int a =o1.getValue().compareTo(o2.getValue()) ;
                if (a == 1)
                	a = -1;
                else if (a == -1)
                	a = 1;
                return a ;
            }
            
        });
        String s = "";
        //将排好序的存入到LinkedHashMap(可保持顺序)中，需要存储键和值信息对到新的映射中。
        Map<String,Integer> linkMap = new LinkedHashMap<>();
        for(Entry<String,Integer> newEntry :result){
            linkMap.put(newEntry.getKey(), newEntry.getValue());            
        }
        //根据entrySet()方法遍历linkMap
        for(Map.Entry<String, Integer> mapEntry : linkMap.entrySet()){
           s+=("key:"+mapEntry.getKey()+"  value:"+mapEntry.getValue()/sum+"\001");
        }
		return s;
    }
    private static class SQLReducer extends Reducer<Text, Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        	Map<String,Integer>  focus = new HashMap<String,Integer> ();
        	double sum = 0;
        	 for(Text val:values)
        	 {
        		 sum += 1;
        		 if (!focus.containsKey(val.toString()))
        		 {
        			 
        			 focus.put(val.toString(), 1);
        		 }
        		 else
        		 {
        			 focus.put(val.toString(), focus.get(val.toString())+1);
        		 }
        	 }
        	 context.write(key, new Text(sortMap(focus,sum)));
    }
    }
    public static void main(String[] args) throws Exception{

    	
        Configuration conf = new Configuration() ;
        DBConfiguration.configureDB(
                conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.235.55/Seeing_future?&autoReconnect=true&failOverReadOnly=false",//原来的,改回来记得改密码
//                "jdbc:mysql://192.168.1.102/movie?&autoReconnect=true&failOverReadOnly=false",//展示的
                "root","iiip");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000/user/hadoopnew/output"), conf);  
        
        //如果输出目录存在就删除  
        if (fileSystem.exists(new Path("hdfs://localhost:9000/user/hadoopnew/output"))){  
            fileSystem.delete(new Path("hdfs://localhost:9000/user/hadoopnew/output"),true);  
        }  
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
          System.err.println("Usage: wordcount <in> <out>");
          System.exit(2);
        }
        Job job = Job.getInstance(conf,"mysql test") ;
//        job.addFileToClassPath(new Path("/user/hadoopnew/lib/mysql-connector-java-5.1.44-bin.jar"));

        job.setJarByClass(WordCount.class);
        job.setMapperClass(SQLMapper.class);
        job.setReducerClass(SQLReducer.class);

//        System.out.print("..");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(DBInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0])); 
        String[] fields = {"movie_id","content","score"} ; 
        DBInputFormat.setInput(
                job,                // job
                MysqlRecord.class,  // input class
                "douban_comment",      // table name
//                "movie_id = 26861685 or movie_id = 26698897",      
                "",// condition
                "movie_id",             // order by
                fields);            // fields
 
        job.waitForCompletion(true) ;
    }
 
}