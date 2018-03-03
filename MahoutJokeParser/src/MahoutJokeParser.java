import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MahoutJokeParser {
    
    public static  class JokesParserMapper extends 
    Mapper<Object, Text, Text, Text> {
        
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                     throws IOException, InterruptedException {

       
 //       public void map(Object key, Text value, Context context)
 //               throws IOException, InterruptedException {
                    
            //Converting the record (single line) to String and storing it in a String variable line
            String line = value.toString();
            String[] pieces = line.split(",");
            if(pieces == null)
               return;
            String jokeID = pieces[0];
            String JokesRating = pieces[1];
            System.out.println("jokeID :" + jokeID +"JokeRating:"  + JokesRating );
            
            context.write( new Text("/" + "jokeID" + "/" + jokeID), new Text(JokesRating));
                        
        }
    }
    /**
     * @param args
     * @throws IOException 
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //Creating a JobConf object and assigning a job name for identification purposes
       
         Job job = new Job(conf, "Jokes Parser");
         job.setJarByClass(MahoutJokeParser.class);
         job.setMapperClass(JokesParserMapper.class);
         job.setNumReduceTasks(0);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);
         job.setInputFormatClass(TextInputFormat.class);
         job.setOutputFormatClass(SequenceFileOutputFormat.class);
         
         Path inputPath = new Path(otherArgs[0]);
         TextInputFormat.addInputPath(job,inputPath);
         
         Path outPath = new Path(otherArgs[1]);
         
         SequenceFileOutputFormat.setOutputPath(job, outPath);
         System.exit(job.waitForCompletion(true) ?0 :1);

    }

}
