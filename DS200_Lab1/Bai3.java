import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai3 {

    //MAPPER
    public static class GenderRatingMapper extends Mapper<Object, Text, Text, Text> {
        private HashMap<String, String> userGenderMap = new HashMap<>();
        private HashMap<String, String> movieTitleMap = new HashMap<>();
        
        private Text titleKey = new Text();
        private Text genderRatingValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    String path = uri.toString();
                    
                    if (path.contains("users.txt")) {
                        BufferedReader reader = new BufferedReader(new FileReader("users.txt"));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] parts = line.split(",", -1);
                            if (parts.length >= 2) {
                                userGenderMap.put(parts[0].trim(), parts[1].trim());
                            }
                        }
                        reader.close();
                    } 
                    else if (path.contains("movies.txt")) {
                        BufferedReader reader = new BufferedReader(new FileReader("movies.txt"));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] parts = line.split(",", 3);
                            if (parts.length >= 2) {
                                movieTitleMap.put(parts[0].trim(), parts[1].trim());
                            }
                        }
                        reader.close();
                    }
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 3) return;

            try {
                String userID = parts[0].trim();
                String movieID = parts[1].trim();
                String rating = parts[2].trim();

                String gender = userGenderMap.get(userID);
                String title = movieTitleMap.get(movieID);

                if (gender != null && title != null) {
                    // ĐÃ SỬA: Chỉ lấy tên phim, không thêm dấu ":" nữa
                    titleKey.set(title); 
                    genderRatingValue.set(gender + "_" + rating);
                    context.write(titleKey, genderRatingValue);
                }
            } catch (Exception e) {}
        }
    }

    //REDUCER
    public static class GenderRatingReducer extends Reducer<Text, Text, Text, Text> {
        private Text resultValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumM = 0.0, sumF = 0.0;
            int countM = 0, countF = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("_");
                if (parts.length == 2) {
                    String gender = parts[0];
                    double rating = Double.parseDouble(parts[1]);
                    
                    if (gender.equalsIgnoreCase("M")) {
                        sumM += rating;
                        countM++;
                    } else if (gender.equalsIgnoreCase("F")) {
                        sumF += rating;
                        countF++;
                    }
                }
            }

            String avgM = (countM > 0) ? String.format(Locale.US, "%.2f", sumM / countM) : "N/A";
            String avgF = (countF > 0) ? String.format(Locale.US, "%.2f", sumF / countF) : "N/A";

            // ĐÃ SỬA: Format chính xác "Male: x.xx, Female: y.yy"
            resultValue.set(String.format("Male: %s, Female: %s", avgM, avgF));
            
            context.write(key, resultValue);
        }
    }

    //MAIN
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Cú pháp: Bai3 <thư_mục_ratings> <đường_dẫn_movies.txt> <đường_dẫn_users.txt> <thư_mục_output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        
        conf.set("mapreduce.output.textoutputformat.separator", " "); 
        
        Job job = Job.getInstance(conf, "Gender Rating Analysis");

        job.setJarByClass(Bai3.class);
        
        job.addCacheFile(new URI(args[1] + "#movies.txt"));
        job.addCacheFile(new URI(args[2] + "#users.txt"));

        job.setMapperClass(GenderRatingMapper.class);
        job.setReducerClass(GenderRatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}