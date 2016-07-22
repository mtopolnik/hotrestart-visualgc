package com.hazelcast.hotrestart.visualgc;

import javax.swing.*;
import java.awt.*;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

/**
 * Swing application which visualizes in real time the dynamically changing contents of a Hot Restart
 * Store snapshot file being written by {@code com.hazelcast.spi.hotrestart.impl.gc.Snapshotter}.
 * The main method takes the Hot Restart store home directory as a command-line argument.
 */
public class VisualGC {
    private static final String CHUNK_SNAPSHOT_FNAME = "chunk-snapshot.bin";
    private static final int WINDOW_X = 5;
    private static final int WINDOW_Y = 200;
    private static final int WINDOW_WIDTH = 1000;
    private static final int WINDOW_HEIGHT = 280;
    private static final int CHUNKSIZE_PIXEL_SCALE = 1 << 16;
    private static final int REFRESH_SNAPSHOT_INTERVAL_MILLIS = 10;
    private static final int WAIT_FOR_SNAPSHOT_INTERVAL_MILLIS = 200;

    private MainPanel mainPanel;
    private Snapshot snapshot = new Snapshot();

    public void startVisualizing(String homePath) {
        final File snapshotFile = new File(homePath, CHUNK_SNAPSHOT_FNAME);
        SwingUtilities.invokeLater(() -> {
            mainPanel = new MainPanel();
            buildFrame(mainPanel);
        });
        long seenTimestamp = 0;
        while (true) {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(snapshotFile)))) {
                final long timestamp = in.readLong();
                if (timestamp == seenTimestamp) {
                    in.close();
                    LockSupport.parkNanos(MILLISECONDS.toNanos(REFRESH_SNAPSHOT_INTERVAL_MILLIS));
                    continue;
                }
                seenTimestamp = timestamp;
                snapshot = snapshot.withEnoughCapacity(in.readInt());
                snapshot.refreshFrom(in);
                SwingUtilities.invokeLater(() -> mainPanel.acceptNewSnapshot(snapshot));
            } catch (IOException e) {
                LockSupport.parkNanos(MILLISECONDS.toNanos(WAIT_FOR_SNAPSHOT_INTERVAL_MILLIS));
            }
        }
    }

    private static void buildFrame(JPanel mainPanel) {
        final JFrame frame = new JFrame();
        frame.setBackground(Color.WHITE);
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.setTitle("Hazelcast HotRestart VisualGC");
        frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
        frame.setLayout(new BorderLayout());
        frame.add(mainPanel);
        frame.setVisible(true);
    }

    private static class MainPanel extends JPanel {
        private Snapshot snapshot;

        void acceptNewSnapshot(Snapshot newSnapshot) {
            snapshot = newSnapshot;
            repaint();
        }

        @Override
        public void paintComponent(Graphics g) {
            super.paintComponent(g);
            if (snapshot == null) {
                return;
            }
            g.setColor(getBackground());
            g.fillRect(0, 0, getWidth(), getHeight());
            for (int i = 0; i < snapshot.chunkCount(); i++) {
                paintBar(g, i, snapshot.sizeAt(i), snapshot.garbageAt(i),
                        snapshot.isSelectedForGcAt(i), snapshot.isGcSurvivorAt(i), snapshot.isTombstoneChunkAt(i));
            }
        }

        private void paintBar(
                Graphics g, int location, int size, int garbage,
                boolean isSelectedForGc, boolean isGcSurvivor, boolean isTombstoneChunk
        ) {
            final int panelHeight = getHeight();
            final int fullHeight = size / CHUNKSIZE_PIXEL_SCALE;
            final int garbageHeight = garbage / CHUNKSIZE_PIXEL_SCALE;
            final int liveHeight = fullHeight - garbageHeight;
            g.setColor(isSelectedForGc ? Color.RED
                     : isGcSurvivor ? Color.GREEN
                     : Color.BLUE);
            g.fillRect(location, panelHeight - liveHeight, 1, liveHeight);
            g.setColor(Color.LIGHT_GRAY);
            g.fillRect(location, panelHeight - fullHeight, 1, garbageHeight);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Must specify home directory on the command line");
            System.exit(1);
        }
        new VisualGC().startVisualizing(args[0]);
    }
}
