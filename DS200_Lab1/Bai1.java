import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai1 {

    // 1. RatingMapper
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text ratingValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();   
            if (line.isEmpty()) return; 
            
            String[] parts = line.split(",");
            if (parts.length < 4) return; 

            try {
                String movieID = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());

                movieIdKey.set(movieID);
                ratingValue.set(String.format("Rate: %.2f", rating));
                context.write(movieIdKey, ratingValue);
            } catch (NumberFormatException e) { }
        }
    }
    
    // 2. MovieMapper 
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text movieNameValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return; 
            
            String[] parts = line.split(",", 3);
            if (parts.length < 2) return; 

            String movieID = parts[0].trim();
            String movieName = parts[1].trim();

            movieIdKey.set(movieID);
            movieNameValue.set(String.format("Movie: %s", movieName));
            context.write(movieIdKey, movieNameValue);
        }
    }

    // 3. RatingReducer 
    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        
        // CLASS VARIABLES
        private String maxMovie = "";
        private double maxRating = 0.0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;
            String movieName = "Unknown Title";

            for (Text value : values) {
                String val = value.toString();

                if (val.startsWith("Rate: ")) {
                    String rate = val.replace("Rate: ", "");
                    sum += Double.parseDouble(rate);
                    count++;
                } else if (val.startsWith("Movie: ")) {
                    movieName = val.replace("Movie: ", "");
                }
            }

            if (count > 0) {
                double avg = sum / count;
                
                outputKey.set(String.format("%s", movieName));
                outputValue.set(String.format("Average rating: %.2f (Total ratings: %d)", avg, count));
                context.write(outputKey, outputValue);

                // Track the highest
                if (count >= 5 && avg > maxRating) {
                    maxRating = avg;
                    maxMovie = movieName;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                String finalMessage = String.format("is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings.", maxRating);
                outputKey.set(maxMovie);
                outputValue.set(finalMessage);
                context.write(outputKey, outputValue);
            }
        }
    }

    // 4. MAIN
    public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Analysis Final");

        job.setJarByClass(bai1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setReducerClass(RatingReducer.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}