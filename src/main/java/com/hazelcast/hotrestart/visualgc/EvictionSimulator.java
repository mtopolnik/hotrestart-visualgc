package com.hazelcast.hotrestart.visualgc;

import javax.swing.*;
import java.awt.*;
import java.util.Arrays;

import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

/**
 * Swing application which visualizes in real time the dynamically changing contents of a Hot Restart
 * Store snapshot file being written by {@code com.hazelcast.spi.hotrestart.impl.gc.Snapshotter}.
 * The main method takes the Hot Restart store home directory as a command-line argument.
 */
public class EvictionSimulator {
    private static final int WINDOW_X = 200;
    private static final int WINDOW_Y = 200;
    private static final int WINDOW_WIDTH = 1000;
    private static final int WINDOW_HEIGHT = 280;
    private static final int BAR_HEIGHT_SCALE = 128;
    private static final int REFRESH_SNAPSHOT_INTERVAL_MILLIS = 20;

    private MainPanel mainPanel;
    private final int sampleSize = 15;
    private final int recordCount = 1000000;
    private final int chunkCount = 1000;

    private final double[] snapshot = new double[chunkCount];
    private final double[] evictionFunction = new double[chunkCount];

    public void startVisualizing() {
        Arrays.fill(snapshot, 1);
        initializeEvictionFunction();
        SwingUtilities.invokeLater(() -> {
            mainPanel = new MainPanel();
            buildFrame(mainPanel);
            new Timer(REFRESH_SNAPSHOT_INTERVAL_MILLIS, (e) -> mainPanel.evict()).start();
        });
    }

    private void initializeEvictionFunction() {
        final int recordsPerChunk = recordCount / chunkCount;
        for (int i = 0; i < evictionFunction.length; i++) {
            double d = 0;
            for (int j = 0; j < recordsPerChunk; j++) {
                d += evictionFunction(recordsPerChunk * i + j);
            }
            evictionFunction[i] = d;
        }
    }

    // Calculates e(x) = ( (n - 1 - x) choose (m - 1) ) / (n choose m)
    // where n is recordCount, m is sampleSize.
    // e(x) gives the probability of x being the minimum element in a random sample
    // of `sampleSize` integers from the range [0 .. `recordCount`].
    private double evictionFunction(int x) {
        double d = (double) sampleSize / recordCount;
        int numerator = recordCount - Math.max(x, sampleSize);
        int denominator = recordCount - 1;
        for (int i = 0; i < Math.min(x, sampleSize); i++) {
            d *= numerator--;
            d /= denominator--;
        }
        return d;
    }

    private static void buildFrame(JPanel mainPanel) {
        final JFrame frame = new JFrame();
        frame.setBackground(Color.WHITE);
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.setTitle("Eviction simulation");
        frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
        frame.setLayout(new BorderLayout());
        frame.add(mainPanel);
        frame.setVisible(true);
    }

    private class MainPanel extends JPanel {
        void evict() {
            double norm = 0;
            for (int i = 0; i < chunkCount; i++) {
                norm += snapshot[i] * snapshot[i];
            }
            final double normalizer = chunkCount / norm;
            for (int i = 0; i < chunkCount; i++) {
                snapshot[i] *= 1 - evictionFunction[i] * snapshot[i] * snapshot[i] * normalizer;
            }
            repaint();
        }

        @Override
        public void paintComponent(Graphics g) {
            super.paintComponent(g);
            g.setColor(getBackground());
            g.fillRect(0, 0, getWidth(), getHeight());
            for (int i = 0; i < snapshot.length; i++) {
                paintBar(g, i, snapshot[i]);
            }
        }

        private void paintBar(Graphics g, int location, double size) {
            final int panelHeight = getHeight();
            final int height = (int) (size * BAR_HEIGHT_SCALE);
            g.setColor(Color.BLUE);
            g.fillRect(location, panelHeight - height, 1, height);
        }
    }

    public static void main(String[] args) throws Exception {
        new EvictionSimulator().startVisualizing();
    }
}
