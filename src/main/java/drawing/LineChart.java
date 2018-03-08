package drawing;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import topologyBuilder.Constants;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class LineChart
{
    public static void drawLineChart(DefaultCategoryDataset countList, String word, long round,
                                     String country, String drawFilePath ) throws IOException {
        DateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm");
        String date = df.format(new Date(new Long(round) * 12*60*1000));

        JFreeChart lineChartObject = ChartFactory.createLineChart(
                "Count graph for \"" + word + "\" in " + date + " in " + country,
                "Document order",
                "count Value",
                countList,
                PlotOrientation.VERTICAL,
                true,true,false);

        int width = 2560; /* Width of the image */
        int height = 960; /* Height of the image */
//    File lineChart = new File(drawFilePath.replace("/","//") + word.replace("/", "-") + "-" + country + "-" + date +".jpeg" );
        File lineChart = new File(drawFilePath.replace("/","//") + country + "-" + round + "-" + word.replace("/", "-") +".jpeg" );
        ChartUtilities.saveChartAsJPEG(lineChart ,lineChartObject, width ,height);
    }
//    public static void main( String[ ] args )
//    {
//        DefaultCategoryDataset vals_test = new DefaultCategoryDataset();
//        DateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm");
//        vals_test.addValue(0L, "count", df.format(new Date(new Long(2034735) * 12*60*1000)));
//        vals_test.addValue(2L, "count", df.format(new Date(new Long(2034736) * 12*60*1000)));
//        vals_test.addValue(5L, "count", df.format(new Date(new Long(2034737) * 12*60*1000)));
//        vals_test.addValue(3L, "count", df.format(new Date(new Long(2034738) * 12*60*1000)));
//        vals_test.addValue(0L, "count", df.format(new Date(new Long(2034739) * 12*60*1000)));
//        vals_test.addValue(0L, "count", df.format(new Date(new Long(2034740) * 12*60*1000)));
//        vals_test.addValue(4L, "count", df.format(new Date(new Long(2034741) * 12*60*1000)));
//        vals_test.addValue(7L, "count", df.format(new Date(new Long(2034742) * 12*60*1000)));
//        vals_test.addValue(5L, "count", df.format(new Date(new Long(2034743) * 12*60*1000)));
//        vals_test.addValue(1L, "count", df.format(new Date(new Long(2034744) * 12*60*1000)));
//        vals_test.addValue(5L, "count", df.format(new Date(new Long(2034745) * 12*60*1000)));
//        vals_test.addValue(0L, "count", df.format(new Date(new Long(2034746) * 12*60*1000)));
//        vals_test.addValue(0L, "count", df.format(new Date(new Long(2034747) * 12*60*1000)));
//        vals_test.addValue(1L, "count", df.format(new Date(new Long(2034748) * 12*60*1000)));
//        vals_test.addValue(0L, "count", df.format(new Date(new Long(2034749) * 12*60*1000)));
//
//        try {
//            drawLineChart(vals_test,"test",2034749,"USA", Constants.IMAGES_FILE_PATH);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }



//    public static void main( String[ ] args )
//    {
//        Stack<Integer> x = new Stack<>();
//        Queue<Integer> y = new LinkedList();
//
//        x.push(1);x.push(2);x.push(3);x.push(4);x.push(5);
//        int o = x.size();
//
//        for (int i = 0; i < o; i++) y.add(x.pop());
//        for (int i = 0; i < o/2; i++)  y.add(y.remove());
//        for (int i = o/2; i < o; i++) x.push(y.remove());
//
//        for (int i = 0; i < (o+1)/2; i++) {
//            System.out.print(x.pop() + ",");
//            if(y.size()>0) System.out.print(y.remove() + ",");
//        }
//
//
//
//    }


    public static void main( String[ ] args )
    {
        List<Integer> init = new ArrayList<>();
        init.add(1);
        init.add(2);
        init.add(3);
        init.add(4);
        init.add(5);





    }

}