package eventDetector.drawing;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.NumberTickUnit;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.CategoryItemRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import topologyBuilder.Constants;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

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
    File lineChart = new File(drawFilePath.replace("/","//") + word.replace("/", "-") + "-" + country + "-" + date +".jpeg" );
    ChartUtilities.saveChartAsJPEG(lineChart ,lineChartObject, width ,height);
  }

  public static void drawScatterChart(XYSeriesCollection result, String drawFilePath ) throws IOException {
    JFreeChart lineChartObject = ChartFactory.createScatterPlot(
            "Process Times",
            "Bolt numbers",
            "Time",
            result);

    for(int i = 0; i < result.getSeries().size(); i++){

        lineChartObject.getXYPlot().getRenderer().setSeriesVisibleInLegend(i, Boolean.FALSE);

    }

    NumberAxis domain = (NumberAxis) lineChartObject.getXYPlot().getDomainAxis();
    domain.setRange(0, 96000);
    domain.setTickUnit(new NumberTickUnit(1));
    domain.setVerticalTickLabels(true);
    NumberAxis range = (NumberAxis) lineChartObject.getXYPlot().getRangeAxis();
    range.setRange(0.0, 30);
    range.setTickUnit(new NumberTickUnit(1));

//    XYItemRenderer renderer = lineChartObject.getXYPlot().getRenderer();
//    renderer.setSeriesItemLabelsVisible(0, false);

    int width = 2560; /* Width of the image */
    int height = 960; /* Height of the image */
    File lineChart = new File(drawFilePath.replace("/","//") + ".jpeg" );
    ChartUtilities.saveChartAsJPEG(lineChart ,lineChartObject, width ,height);
  }

  public static void main( String[ ] args )
  {
    DefaultCategoryDataset vals_test = new DefaultCategoryDataset();
    DateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm");
    vals_test.addValue(0L, "count", df.format(new Date(new Long(2034735) * 12*60*1000)));
    vals_test.addValue(2L, "count", df.format(new Date(new Long(2034736) * 12*60*1000)));
    vals_test.addValue(5L, "count", df.format(new Date(new Long(2034737) * 12*60*1000)));
    vals_test.addValue(3L, "count", df.format(new Date(new Long(2034738) * 12*60*1000)));
    vals_test.addValue(0L, "count", df.format(new Date(new Long(2034739) * 12*60*1000)));
    vals_test.addValue(0L, "count", df.format(new Date(new Long(2034740) * 12*60*1000)));
    vals_test.addValue(4L, "count", df.format(new Date(new Long(2034741) * 12*60*1000)));
    vals_test.addValue(7L, "count", df.format(new Date(new Long(2034742) * 12*60*1000)));
    vals_test.addValue(5L, "count", df.format(new Date(new Long(2034743) * 12*60*1000)));
    vals_test.addValue(1L, "count", df.format(new Date(new Long(2034744) * 12*60*1000)));
    vals_test.addValue(5L, "count", df.format(new Date(new Long(2034745) * 12*60*1000)));
    vals_test.addValue(0L, "count", df.format(new Date(new Long(2034746) * 12*60*1000)));
    vals_test.addValue(0L, "count", df.format(new Date(new Long(2034747) * 12*60*1000)));
    vals_test.addValue(1L, "count", df.format(new Date(new Long(2034748) * 12*60*1000)));
    vals_test.addValue(0L, "count", df.format(new Date(new Long(2034749) * 12*60*1000)));

    try {
      drawLineChart(vals_test,"test",2034749,"USA", Constants.IMAGES_FILE_PATH);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}