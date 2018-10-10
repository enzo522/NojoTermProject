import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class NojoTermProject {
    public static class DustMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object jsonFile, Text data, Context ctx) throws IOException, InterruptedException {
            Text key = new Text();
            Text value = new Text();

            try {
                JSONParser jp = new JSONParser();
                JSONObject jo = (JSONObject) jp.parse(data.toString());
                JSONObject json = (JSONObject) jo.get("DailyAverageAirQuality");
                JSONArray dailyData = (JSONArray) json.get("row");

                for(int i = 0; i < dailyData.size(); i++) {
                    JSONObject observ = (JSONObject) dailyData.get(i);

                    key.set(observ.get("MSRSTE_NM").toString());
                    value.set(observ.get("MSRDT_DE").toString() + "/" + observ.get("PM10").toString());

                    ctx.write(key, value);
                }
            } catch(ParseException pe) {
                pe.printStackTrace();
            }
        }
    }

    public static class DustReducer extends Reducer <Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            Configuration conf = ctx.getConfiguration();
            Text value = new Text();

            Vector<Vector<Vector<Double>>> datas = new Vector<Vector<Vector<Double>>>();
            Vector<Vector<Double>> monthlyAvgs = new Vector<Vector<Double>>();

            String date = null;
            String pm = null;
            StringBuilder year = null;
            StringBuilder month = null;

            double monthlySum = 0.0;
            int monthlyCount = 0;
            int duration = Integer.parseInt(conf.get("endYear")) - Integer.parseInt(conf.get("startYear")) + 1;

            for(int i = 0; i < duration; i++) {
                datas.add(new Vector<Vector<Double>>());

                for(int j = 0; j < 12; j++)
                    datas.elementAt(i).add(new Vector<Double>());
            }

            for(Text t : values) {
                String[] line = t.toString().split("/");
                date = line[0];
                pm = line[1];

                if(!pm.equals("0.0")) {
                    year = new StringBuilder(date.charAt(0) + "" + date.charAt(1) + "" + date.charAt(2) + "" + date.charAt(3));
                    month = new StringBuilder(date.charAt(4) + "" + date.charAt(5));

                    for(int i = 0; i < duration; i++) {
                        if(Integer.parseInt(year.toString()) == Integer.parseInt(conf.get("startYear")) + i) {
                            for(int j = 0; j < 12; j++) {
                                if(Integer.parseInt(month.toString()) == (j + 1))
                                    datas.elementAt(i).elementAt(j).add(Double.parseDouble(pm));
                            }
                        }
                    }
                }
            }

            for(int i = 0; i < datas.size(); i++) {
                monthlyAvgs.add(new Vector<Double>());

                for(int j = 0; j < datas.elementAt(i).size(); j++) {
                    monthlySum = 0.0;
                    monthlyCount = 0;

                    for(int k = 0; k < datas.elementAt(i).elementAt(j).size(); k++) {
                        monthlySum += datas.elementAt(i).elementAt(j).elementAt(k);
                        monthlyCount++;
                    }

                    if(monthlyCount != 0)
                        monthlyAvgs.elementAt(i).add(Math.round((monthlySum / monthlyCount) * 10d) / 10d);
                    else
                        monthlyAvgs.elementAt(i).add(0.0);
                }
            }

            for(int i = 0; i < duration; i++) {
                for(int j = 0; j < monthlyAvgs.elementAt(i).size(); j++) {
                    if(monthlyAvgs.elementAt(i).elementAt(j) != 0.0) {
                        value.set((Integer.parseInt(conf.get("startYear")) + i) + "년 " + (j + 1) + "월 평균: " + monthlyAvgs.elementAt(i).elementAt(j));
                        ctx.write(key, value);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if(args.length != 4) {
            System.err.println("Usage: nojo.TermProject.NojoTermProject <inputDir> <outputDir> <startYear> <endYear>");
            System.exit(2);
        }

        if(Integer.parseInt(args[2]) > Integer.parseInt(args[3])) {
            System.err.println("시작 연도가 종료 연도보다 클 수 없습니다.");
            System.exit(3);
        }

        conf.set("startYear", args[2]);
        conf.set("endYear", args[3]);

        Job job = new Job(conf, "Nojo Term Project");

        job.setJarByClass(NojoTermProject.class);
        job.setMapperClass(DustMapper.class);
        job.setReducerClass(DustReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if(job.waitForCompletion(true)) {
            Runtime rt = Runtime.getRuntime();
            rt.exec("/home/hadoop/hadoop/bin/hadoop jar NojoTermProject.jar nojo.TermProject.Visualizer " + args[1]);
        }
    }
}