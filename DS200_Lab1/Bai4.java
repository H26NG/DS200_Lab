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

public class Bai4 {

    //MAPPER
    public static class AgeGroupRatingMapper extends Mapper<Object, Text, Text, Text> {
        private HashMap<String, String> userAgeMap = new HashMap<>();
        private HashMap<String, String> movieTitleMap = new HashMap<>();
        
        private Text titleKey = new Text();
        private Text ageRatingValue = new Text();

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
                            // Cột Age nằm ở vị trí số 3 (index 2)
                            if (parts.length >= 3) {
                                userAgeMap.put(parts[0].trim(), parts[2].trim());
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

                String ageStr = userAgeMap.get(userID);
                String title = movieTitleMap.get(movieID);

                if (ageStr != null && title != null) {
                    int age = Integer.parseInt(ageStr);
                    String ageGroup = "";

                    // Chia nhóm độ tuổi
                    if (age <= 18) {
                        ageGroup = "0-18";
                    } else if (age <= 35) {
                        ageGroup = "18-35";
                    } else if (age <= 50) {
                        ageGroup = "35-50";
                    } else {
                        ageGroup = "50+";
                    }

                    titleKey.set(title); 
                    ageRatingValue.set(ageGroup + "_" + rating);
                    context.write(titleKey, ageRatingValue);
                }
            } catch (Exception e) {}
        }
    }

    //REDUCER
    public static class AgeGroupRatingReducer extends Reducer<Text, Text, Text, Text> {
        private Text resultValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum_0_18 = 0.0, sum_18_35 = 0.0, sum_35_50 = 0.0, sum_50_plus = 0.0;
            int count_0_18 = 0, count_18_35 = 0, count_35_50 = 0, count_50_plus = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("_");
                if (parts.length == 2) {
                    String ageGroup = parts[0];
                    double rating = Double.parseDouble(parts[1]);
                    
                    if (ageGroup.equals("0-18")) {
                        sum_0_18 += rating;
                        count_0_18++;
                    } else if (ageGroup.equals("18-35")) {
                        sum_18_35 += rating;
                        count_18_35++;
                    } else if (ageGroup.equals("35-50")) {
                        sum_35_50 += rating;
                        count_35_50++;
                    } else if (ageGroup.equals("50+")) {
                        sum_50_plus += rating;
                        count_50_plus++;
                    }
                }
            }

            // Tính trung bình, nếu count = 0 thì gán là "NA"
            String avg_0_18 = (count_0_18 > 0) ? String.format(Locale.US, "%.2f", sum_0_18 / count_0_18) : "NA";
            String avg_18_35 = (count_18_35 > 0) ? String.format(Locale.US, "%.2f", sum_18_35 / count_18_35) : "NA";
            String avg_35_50 = (count_35_50 > 0) ? String.format(Locale.US, "%.2f", sum_35_50 / count_35_50) : "NA";
            String avg_50_plus = (count_50_plus > 0) ? String.format(Locale.US, "%.2f", sum_50_plus / count_50_plus) : "NA";

            resultValue.set(String.format("0-18: %s 18-35: %s 35-50: %s 50+: %s", avg_0_18, avg_18_35, avg_35_50, avg_50_plus));
            
            context.write(key, resultValue);
        }
    }

    // MAIN
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Cú pháp: Bai4 <thư_mục_ratings> <đường_dẫn_movies.txt> <đường_dẫn_users.txt> <thư_mục_output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // Dùng dấu cách để nối Key (Tên phim) và Value
        conf.set("mapreduce.output.textoutputformat.separator", " "); 
        
        Job job = Job.getInstance(conf, "Age Group Rating Analysis");

        job.setJarByClass(Bai4.class);
        
        job.addCacheFile(new URI(args[1] + "#movies.txt"));
        job.addCacheFile(new URI(args[2] + "#users.txt"));

        job.setMapperClass(AgeGroupRatingMapper.class);
        job.setReducerClass(AgeGroupRatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}