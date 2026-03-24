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

public class Bai2 {

    public static class GenreRatingMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private HashMap<String, String[]> movieGenres = new HashMap<>();
        private Text genreKey = new Text();
        private DoubleWritable ratingValue = new DoubleWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader("movies.txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 3); 
                    if (parts.length >= 3) {
                        String movieID = parts[0].trim();
                        String[] genres = parts[2].trim().split("\\|"); 
                        movieGenres.put(movieID, genres);
                    }
                }
                reader.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // TRẢ LẠI DẤU PHẨY (,) Ở ĐÂY
            String[] parts = line.split(",");
            if (parts.length < 3) return;

            try {
                String movieID = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());

                String[] genres = movieGenres.get(movieID);
                if (genres != null) {
                    for (String genre : genres) {
                        genreKey.set(genre);
                        ratingValue.set(rating);
                        context.write(genreKey, ratingValue);
                    }
                }
            } catch (NumberFormatException e) { }
        }
    }

    public static class AverageGenreReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count > 0) {
                double avg = sum / count;
                outputValue.set(String.format(Locale.US, "Avg: %.2f,  Count: %d", avg, count));
                context.write(key, outputValue); 
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Cú pháp: Bai2 <thư_mục_ratings> <đường_dẫn_movies.txt> <thư_mục_output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");

        Job job = Job.getInstance(conf, "Genre Rating Analysis");
        job.setJarByClass(Bai2.class);
        job.addCacheFile(new URI(args[1] + "#movies.txt"));
        job.setMapperClass(GenreRatingMapper.class);
        job.setReducerClass(AverageGenreReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}