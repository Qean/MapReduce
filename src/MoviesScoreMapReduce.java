import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.exit;

public class MoviesScoreMapReduce {
    //Handles output form previous job
    public static final class MoviesMapper1 extends Mapper<Object, Text, Text, Text> {
        private Text filmGenre = new Text();
        private Text genreMeanScore = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) {
            try {
                String[] str = value.toString().split("\t");
                filmGenre.set(str[0]);
                genreMeanScore.set(str[1]);
                System.out.println("Key: " + filmGenre + " Value: " + genreMeanScore);
                context.write(filmGenre, genreMeanScore);

            } catch (Exception e) {
                e.fillInStackTrace();
                System.err.println(e.getMessage());
                e.printStackTrace();
                exit(1);
            }
        }
    }

    //Handles dataset
    public static final class MoviesMapper2 extends Mapper<Object, Text, Text, Text> {
        private Text filmGenre = new Text();
        private Text genreAndFilmMeanScore = new Text();
        private String[] str;
        float tempScore;
        private String titleTemp;

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) {
            try {

                //Parse out genre as a key
                String cleanString = value.toString();
                //Ignores ',' in quotes
                str = cleanString.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

                //Skip first column
                if (str[5].equals("genre"))
                    return;

                //Parse out mean score and film title as a value
                Pattern meanScorePattern = Pattern.compile("[0-9]*\\.[0-9]");
                Matcher matcher = meanScorePattern.matcher(value.toString());
                while (matcher.find()) {
                    float digits = Float.parseFloat(matcher.group());
                    if (digits <= 10.0 && digits > 1.0) {
                        tempScore = digits;
                        break;
                    }
                }

                //Some genres got multiple genres in string
                String keyTemp = str[5];
                titleTemp = "\"" + str[1].replaceAll("\"", "") + "\"";
                keyTemp = keyTemp.replaceAll(" ", "");
                keyTemp = keyTemp.replaceAll("\"", "");
                for (String str : keyTemp.split(",")
                ) {
                    filmGenre.set(str);
                    genreAndFilmMeanScore.set(titleTemp + "," + tempScore);
                    System.out.println("Key: " + filmGenre + " Value: " + genreAndFilmMeanScore);
                    context.write(filmGenre, genreAndFilmMeanScore);
                }

            } catch (Exception e) {
                e.fillInStackTrace();
                System.err.println(e.getMessage());
                System.err.println(Arrays.toString(str));
                System.err.println("Key: " + filmGenre.toString() + " Value: " + genreAndFilmMeanScore.toString());
                e.printStackTrace();
                exit(1);
            }
        }
    }

    public static class MoviesReducer extends Reducer<Text, Text, Text, Text> {
        Text filmTitle = new Text();
        Text filmMeanScore = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            try {
                LinkedList<String> titleAndMeanScore = new LinkedList<>();
                float genreMeanScore = 0;
                for (Text val : values) {
                    System.out.println("Key: " + key + " Value: " + val);
                    String stringVal = val.toString();
                    char firstChar = (char) val.charAt(0);
                    char secondChar = (char) val.charAt(1);

                    //Checks if it's output from Mapper1 or 2
                    if (Character.isDigit((firstChar)) && secondChar == '.') {
                        genreMeanScore = Float.parseFloat(stringVal);
                        context.write(key, new Text(stringVal));
                    } else {
                        titleAndMeanScore.add(stringVal);
                    }
                }
                float finalGenreMeanScore = genreMeanScore;

                //Sorts film titles in genre descending
                titleAndMeanScore.sort(Collections.reverseOrder(new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        String[] t1 = o1.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                        String[] t2 = o2.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

                        o1 = t1[1];
                        o2 = t2[1];
                        return Float.compare(Float.parseFloat(o1), Float.parseFloat(o2));
                    }
                }));

                //Switches out keys, genre for film title and context.write's them
                titleAndMeanScore.forEach(element -> {
                    String[] str = element.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                    float meanScore = Float.parseFloat(str[1]);
                    if (meanScore >= finalGenreMeanScore) {
                        filmTitle.set(str[0]);
                        filmMeanScore.set(str[1]);
                        try {
                            context.write(filmTitle, filmMeanScore);
                        } catch (Exception e) {
                            e.fillInStackTrace();
                            System.err.println(e.getMessage());
                            e.printStackTrace();
                            exit(1);
                        }
                    }
                });
            } catch (Exception e) {
                e.fillInStackTrace();
                System.err.println(e.getMessage());
                for (Text val : values) {
                    System.err.println("Key: " + key.toString() + " Value: " + val.toString());
                }
                e.printStackTrace();
                exit(1);
            }
        }
    }

    public static class MeanPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
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
