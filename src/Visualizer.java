import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.Font;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.DefaultListCellRenderer;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.ListSelectionModel;
import javax.swing.LookAndFeel;
import javax.swing.UIManager;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.plaf.ColorUIResource;
import javax.swing.plaf.basic.BasicScrollBarUI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.CategoryItemLabelGenerator;
import org.jfree.chart.labels.ItemLabelAnchor;
import org.jfree.chart.labels.ItemLabelPosition;
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.chart.renderer.category.StandardBarPainter;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.TextAnchor;

public class Visualizer {
    public static class Observatory {
        private String observName = null;
        private Vector<String> dates = new Vector<String>();
        private Vector<Double> avgs = new Vector<Double>();

        public Observatory(String observName) {
            this.observName = observName;
        }

        public void append(String date, Double avg) {
            this.dates.add(date);
            this.avgs.add(avg);
        }

        public String getObservName() {
            return this.observName;
        }

        public Vector<String> getDate() {
            return this.dates;
        }

        public Vector<Double> getAvg() {
            return this.avgs;
        }

        public String toString() {
            return this.observName;
        }
    }

    public static class CustomScrollBar extends BasicScrollBarUI {
        protected void configureScrollBarColors() {
            LookAndFeel.installColors(scrollbar, "ScrollBar.background",
                    "ScrollBar.foreground");
            thumbHighlightColor = UIManager.getColor("ScrollBar.thumbHighlight");
            thumbLightShadowColor = UIManager.getColor("ScrollBar.thumbShadow");
            thumbDarkShadowColor = UIManager.getColor("ScrollBar.thumbDarkShadow");
            thumbColor = UIManager.getColor("ScrollBar.thumb");
            trackColor = UIManager.getColor("ScrollBar.track");
            trackHighlightColor = UIManager.getColor("ScrollBar.trackHighlight");
        }

        protected JButton createDecreaseButton(int orientation) {
            return createZeroButton();
        }

        protected JButton createIncreaseButton(int orientation) {
            return createZeroButton();
        }

        private JButton createZeroButton() {
            JButton jButton = new JButton();
            jButton.setPreferredSize(new Dimension(0, 0));
            jButton.setMinimumSize(new Dimension(0, 0));
            jButton.setMaximumSize(new Dimension(0, 0));

            return jButton;
        }
    }

    public static class PlotPanel extends JPanel {
        private Vector<String> months;
        private Vector<Double> monthlyAvgs;
        private double mortality = 0.0;

        public PlotPanel(Observatory observatory) {
            this.months = observatory.getDate();
            this.monthlyAvgs = observatory.getAvg();
        }

        public ChartPanel getChart() {
            DefaultCategoryDataset barDataset = new DefaultCategoryDataset();
            DefaultCategoryDataset lineDataset = new DefaultCategoryDataset();

            for (int i = 0; i < this.months.size(); i++) {
                mortality = this.monthlyAvgs.elementAt(i) * 0.11;

                if (mortality > 0.0) {
                    barDataset.addValue(Math.round(mortality * 10d) / 10d,
                            "사망률 기여도 (%)", this.months.elementAt(i));
                } else {
                    barDataset.addValue(0.0, "사망률 기여도 (%)",
                            this.months.elementAt(i));
                }

                lineDataset.addValue(this.monthlyAvgs.elementAt(i),
                        "평균 미세먼지량 (㎍/㎥)", this.months.elementAt(i));
            }

            BarRenderer barRenderer = new BarRenderer();
            BarRenderer.setDefaultBarPainter(new StandardBarPainter());
            LineAndShapeRenderer lineRenderer = new LineAndShapeRenderer();
            CategoryItemLabelGenerator generator =
                    new StandardCategoryItemLabelGenerator();
            ItemLabelPosition pCenter = new ItemLabelPosition
                    (ItemLabelAnchor.OUTSIDE12, TextAnchor.BOTTOM_CENTER);
            Font font = new Font("Monospace", Font.PLAIN, 12);

            barRenderer.setBaseItemLabelGenerator(generator);
            barRenderer.setBaseItemLabelsVisible(true);
            barRenderer.setShadowVisible(false);
            barRenderer.setDataBoundsIncludesVisibleSeriesOnly(false);

            barRenderer.setBasePositiveItemLabelPosition(pCenter);
            barRenderer.setBaseItemLabelFont(font);
            barRenderer.setBaseItemLabelPaint(new Color(255, 255, 255));
            barRenderer.setSeriesPaint(0, new Color(41, 41, 43));
            barRenderer.setGradientPaintTransformer(null);

            lineRenderer.setBaseItemLabelGenerator(generator);
            lineRenderer.setBaseItemLabelsVisible(true);
            lineRenderer.setBaseShapesVisible(true);
            lineRenderer.setDrawOutlines(true);
            lineRenderer.setUseFillPaint(true);

            lineRenderer.setBaseFillPaint(Color.WHITE);
            lineRenderer.setBaseItemLabelPaint(new Color(255, 255, 255));
            lineRenderer.setBaseItemLabelFont(font);
            lineRenderer.setBasePositiveItemLabelPosition(pCenter);
            lineRenderer.setSeriesPaint(0, new Color(219, 121, 22));
            lineRenderer.setSeriesStroke(0, new BasicStroke(2.0f,
                    BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 3.0f));

            CategoryPlot plot = new CategoryPlot();

            plot.setDataset(1, lineDataset);
            plot.setRenderer(1, lineRenderer);
            plot.setDataset(2, barDataset);
            plot.setRenderer(2, barRenderer);

            plot.setOrientation(PlotOrientation.VERTICAL);
            plot.setRangeGridlinesVisible(false);
            plot.setDomainGridlinesVisible(false);
            plot.setOutlineVisible(false);
            plot.setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD);

            plot.setDomainAxis(new CategoryAxis());
            plot.getDomainAxis().setTickLabelFont(font);
            plot.getDomainAxis().setTickLabelPaint(new Color(255, 255, 255));
            plot.getDomainAxis().setCategoryLabelPositions
                    (CategoryLabelPositions.UP_45);

            plot.setRangeAxis(new NumberAxis());
            plot.getRangeAxis().setTickLabelFont(font);
            plot.getRangeAxis().setTickLabelPaint(new Color(255, 255, 255));
            plot.setBackgroundPaint(new Color(93, 99, 151));

            JFreeChart jfreeChart = new JFreeChart(plot);
            jfreeChart.setBackgroundPaint(new Color(93, 99, 151));

            return new ChartPanel(jfreeChart);
        }
    }

    public static class UserInterfacer extends JPanel {
        private final int listWidth = 130;
        private final int plotWidth = 1200;
        private final int height = 700;

        private JFrame frame;
        private JList<Observatory> observList;
        private PlotPanel plotPanel;
        private JScrollPane listScrollPane;
        private JScrollPane chartScrollPane;
        private JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);

        public UserInterfacer(Vector<Observatory> observatorys) {
            frame = new JFrame("미세먼지 그래프");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

            observList = new JList<Observatory>(observatorys);
            observList.setFont(new Font("Monospace", Font.PLAIN, 16));

            observList.setCellRenderer(new DefaultListCellRenderer() {
                public int getHorizontalAlignment() {
                    return CENTER;
                }
            });

            observList.addListSelectionListener(new ListSelectionListener() {
                public void valueChanged(ListSelectionEvent e) {
                    JList<Observatory> selectedObserv =
                            (JList<Observatory>) e.getSource();

                    for (int i = 0; i < observatorys.size(); i++) {
                        if (observatorys.elementAt(i).getObservName()
                                .equals(selectedObserv.getSelectedValue().
                                        getObservName())) {
                            plotPanel.removeAll();
                            chartScrollPane.removeAll();
                            splitPane.removeAll();

                            draw(observatorys.elementAt(i));
                            selectedObserv.setSelectionBackground
                                    (new Color(93, 99, 151));
                            selectedObserv.setSelectionForeground
                                    (new Color(255, 255, 255));
                            splitPane.revalidate();

                            break;
                        }
                    }
                }
            });

            draw(observatorys.elementAt(0));
            observList.setSelectedIndex(0);

            frame.add(splitPane);
            frame.pack();
            frame.setVisible(true);
        }

        public void draw(Observatory observatory) {
            observList.setBackground(new Color(138, 142, 167));
            observList.setForeground(new Color(255, 255, 255));
            observList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

            plotPanel = new PlotPanel(observatory);
            chartScrollPane = new JScrollPane(plotPanel.getChart());

            listScrollPane = new JScrollPane(observList);
            listScrollPane.getVerticalScrollBar().setUI(new CustomScrollBar());

            UIManager.put("ScrollBar.thumb", new ColorUIResource
                    (new Color(110, 124, 160)));
            UIManager.put("ScrollBar.background", new ColorUIResource
                    (new Color(255, 255, 255)));

            listScrollPane.setMinimumSize(new Dimension(listWidth, height));
            chartScrollPane.setMinimumSize(new Dimension(plotWidth, height));
            splitPane.setPreferredSize(new Dimension
                    (listWidth + plotWidth, height));

            listScrollPane.setBorder(javax.swing.BorderFactory.createEmptyBorder());
            chartScrollPane.setBorder(javax.swing.BorderFactory.createEmptyBorder());

            splitPane.add(chartScrollPane);
            splitPane.add(listScrollPane);
            splitPane.setDividerSize(0);
            splitPane.setDividerLocation(plotWidth);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if (args.length != 1) {
            System.err.println("Usage: nojo.TermProject.Visualizer <inputDir>");
            System.exit(2);
        }

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream dis = fs.open(new Path("/user/hadoop/" + args[0]
                + "/part-r-00000"));
        BufferedReader br = new BufferedReader(new InputStreamReader(dis));

        Vector<Observatory> observatorys = new Vector<Observatory>();
        StringTokenizer stz = null;

        String observ = null;
        String line = null;
        String date = null;
        String avg = null;

        int index = -1;
        boolean observExists = false;

        while ((line = br.readLine()) != null) {
            stz = new StringTokenizer(line);

            if (stz.hasMoreTokens()) {
                observ = stz.nextToken("\t");
                date = stz.nextToken(":");
                avg = stz.nextToken("\n");

                for (int i = 0; i < observatorys.size(); i++) {
                    if (observatorys.elementAt(i).getObservName().equals(observ)) {
                        observExists = true;
                        index = i;

                        break;
                    }
                }

                if (observExists)
                    observatorys.elementAt(index).append(
                            date.substring(3, 5) + "/"
                                    + date.substring(7, date.length() - 4),
                            Double.parseDouble(avg.substring(1)));
                else {
                    observatorys.add(new Observatory(observ));
                    observatorys.lastElement().append(date.substring(3, 5)
                                    + "/" + date.substring(7, date.length() - 4),
                            Double.parseDouble(avg.substring(1)));
                }

                observExists = false;
                index = -1;
            }
        }

        EventQueue.invokeLater(new Runnable() {
            public void run() {
                new UserInterfacer(observatorys);
            }
        });
    }
}