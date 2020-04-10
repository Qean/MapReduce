import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.exit;

public class MeanScoreMapReduce {
    public static final class MeanMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private Text filmGenre = new Text();
        private FloatWritable filmMeanScore = new FloatWritable();
        private String[] str;

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, FloatWritable>.Context context) {
            try {
                //Parse out genre as a key
                String cleanString = value.toString();
                //Ignores ',' in quotes
                str = cleanString.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

                //Skip first row
                if (str[5].equals("genre"))
                    return;

                //Parse out mean score for film as a value
                Pattern meanScorePattern = Pattern.compile("([0-9]*\\.[0-9])");
                Matcher matcher = meanScorePattern.matcher(value.toString());

                //The problem is that dataset is inconsistent, some descriptions have quotes around them and some don't
                //This regex accepts only "number.number" but it sometimes pick up other values than score
                //This loop filters out most errors but not all of them
                while (matcher.find()) {
                    float digits = Float.parseFloat(matcher.group());
                    if (digits <= 10.0 && digits > 1.0) {
                        filmMeanScore.set(digits);
                        break;
                    }
                }

                //Some genres columns got multiple genres in quotes, this filters them out and context.write for all of them
                String keyTemp = str[5];
                keyTemp = keyTemp.replaceAll(" ", "");
                keyTemp = keyTemp.replaceAll("\"", "");
                for (String str : keyTemp.split(",")) {
                    filmGenre.set(str);
                    System.out.println("Key: " + filmGenre + " Value: " + filmMeanScore);
                    context.write(filmGenre, filmMeanScore);
                }

            } catch (Exception e) {
                e.fillInStackTrace();
                System.err.println(e.getMessage());
                System.err.println(Arrays.toString(str));
                System.err.println("Key: " + filmGenre.toString() + " Value: " + filmMeanScore.toString());
                e.printStackTrace();
                exit(1);
            }
        }
    }

    public static class MeanReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) {
            try {
                float scores = 0;
                int length = 0;

                for (FloatWritable val : values) {
                    scores += val.get();
                    length++;
                }
                result.set(scores / length);
                context.write(key, result);

            } catch (Exception e) {
                e.fillInStackTrace();
                System.err.println(e.getMessage());
                for (FloatWritable val : values) {
                    System.err.println("Key: " + key.toString() + " Value: " + val.toString());
                }
                e.printStackTrace();
                exit(1);
            }
        }
    }

    public static class MeanPartitioner extends Partitioner<Text, FloatWritable> {

        @Override
        public int getPartition(Text key, FloatWritable value, int numReduceTasks) {
            try {
                System.out.println("Key: " + key + " Value: " + value);
                if (numReduceTasks == 0) {
                    return 0;
                }

                //There are 26 unique genres in this set, one reducer for genre
                switch (key.toString()) {
                    case "Fantasy":
                        return 0;
                    case "History":
                        return 1;
                    case "Animation":
                        return 2;
                    case "Musical":
                        return 3;
                    case "Western":
                        return 4;
                    case "Film-Noir":
                        return 5;
                    case "Adult":
                        return 6;
                    case "Comedy":
                        return 7;
                    case "Music":
                        return 8;
                    case "Sci-Fi":
                        return 9;
                    case "Thriller":
                        return 10;
                    case "Documentary":
                        return 11;
                    case "Mystery":
                        return 12;
                    case "News":
                        return 13;
                    case "Game-Show":
                        return 14;
                    case "War":
                        return 15;
                    case "Reality-TV":
                        return 16;
                    case "Action":
                        return 17;
                    case "Horror":
                        return 18;
                    case "Biography":
                        return 19;
                    case "Crime":
                        return 20;
                    case "Family":
                        return 21;
                    case "Adventure":
                        return 22;
                    case "Drama":
                        return 23;
                    case "Romance":
                        return 24;
                    case "Sport":
                        return 25;
                }

            } catch (Exception e) {
                e.fillInStackTrace();
                System.err.println(e.getMessage());
                System.err.println("Key: " + key.toString() + " Value: " + value.toString());
                e.printStackTrace();
                exit(1);
            }
            return 0;
        }
    }
}
