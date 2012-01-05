package input_format;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultiHFileOutputFormat 
extends FileOutputFormat<ImmutableBytesWritable, KeyValue> {
  static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";

  Map<ImmutableBytesWritable, RecordWriter<ImmutableBytesWritable, KeyValue>> writers =
    new HashMap<ImmutableBytesWritable, RecordWriter<ImmutableBytesWritable, KeyValue>>();

  /* Data structure to hold a Writer and amount of data written on it. */
  static class WriterLength {
    long written = 0;
    HFile.Writer writer = null;
  }

  public RecordWriter<ImmutableBytesWritable, KeyValue> getRecordWriter(final TaskAttemptContext context)
  throws IOException, InterruptedException {
    return new RecordWriter<ImmutableBytesWritable, KeyValue>() {

      @Override
      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {
        for (RecordWriter<ImmutableBytesWritable, KeyValue> writer: writers.values()) {
          writer.close(context);
        }
      }

      @Override
      public void write(ImmutableBytesWritable key, KeyValue value)
          throws IOException, InterruptedException {
        RecordWriter<ImmutableBytesWritable, KeyValue> writer = writers.get(key);
        if (writer == null) {
          final Path outputPath =
            new Path(FileOutputFormat.getOutputPath(context).toString() + "/" +
                Bytes.toString(key.get()));
          writer = new RecordWriter<ImmutableBytesWritable, KeyValue>() {
            final FileOutputCommitter committer =
              new FileOutputCommitter(outputPath, context);
            final Path outputdir = committer.getWorkPath();
            final Configuration conf = context.getConfiguration();
            final FileSystem fs = outputdir.getFileSystem(conf);
            final long maxsize = conf.getLong("hbase.hregion.max.filesize",
                HConstants.DEFAULT_MAX_FILE_SIZE);
            final int blocksize = conf.getInt("hfile.min.blocksize.size",
                HFile.DEFAULT_BLOCKSIZE);
            // Invented config.  Add to hbase-*.xml if other than default compression.
            final String compression = conf.get("hfile.compression",
                Compression.Algorithm.NONE.getName());

            // Map of families to writers and how much has been output on the writer.
            final Map<byte [], WriterLength> writers =
              new TreeMap<byte [], WriterLength>(Bytes.BYTES_COMPARATOR);
            byte [] previousRow = HConstants.EMPTY_BYTE_ARRAY;
            final byte [] now = Bytes.toBytes(System.currentTimeMillis());
            boolean rollRequested = false;

            public void write(ImmutableBytesWritable row, KeyValue kv)
            throws IOException {
              // null input == user explicitly wants to flush
              if (row == null && kv == null) {
                rollWriters();
                return;
              }

              byte [] rowKey = kv.getRow();
              long length = kv.getLength();
              byte [] family = kv.getFamily();
              WriterLength wl = this.writers.get(family);

              // If this is a new column family, verify that the directory exists
              if (wl == null) {
                fs.mkdirs(new Path(outputdir, Bytes.toString(family)));
              }

              // If any of the HFiles for the column families has reached
              // maxsize, we need to roll all the writers
              if (wl != null && wl.written + length >= maxsize) {
                this.rollRequested = true;
              }

              // This can only happen once a row is finished though
              if (rollRequested && Bytes.compareTo(this.previousRow, rowKey) != 0) {
                rollWriters();
              }

              // create a new HLog writer, if necessary
              if (wl == null || wl.writer == null) {
                wl = getNewWriter(family);
              }

              // we now have the proper HLog writer. full steam ahead
              kv.updateLatestStamp(this.now);
              wl.writer.append(kv);
              wl.written += length;

              // Copy the row so we know when a row transition.
              this.previousRow = rowKey;
            }

            private void rollWriters() throws IOException {
              for (WriterLength wl : this.writers.values()) {
                if (wl.writer != null) {
                  close(wl.writer);
                }
                wl.writer = null;
                wl.written = 0;
              }
              this.rollRequested = false;
            }

           private HFile.Writer getNewWriter(final HFile.Writer writer,
                final Path familydir, Configuration conf)
                throws IOException {
              if (writer != null) {
                close(writer);
              }
              return new HFile.Writer(fs,  StoreFile.getUniqueFile(fs, familydir),
                      blocksize, compression, KeyValue.KEY_COMPARATOR);
            }

            private WriterLength getNewWriter(byte[] family) throws IOException {
              WriterLength wl = new WriterLength();
              Path familydir = new Path(outputdir, Bytes.toString(family));
              wl.writer = getNewWriter(wl.writer, familydir, conf); 
              this.writers.put(family, wl);
              return wl;
            }

            private void close(final HFile.Writer w) throws IOException {
              if (w != null) {
                w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
                    Bytes.toBytes(System.currentTimeMillis()));
                w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
                    Bytes.toBytes(context.getTaskAttemptID().toString()));
                w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, 
                    Bytes.toBytes(true));
                w.close();
              }
            }

            public void close(TaskAttemptContext c)
            throws IOException, InterruptedException {
              for (WriterLength wl: this.writers.values()) {
                close(wl.writer);
              }
              committer.commitTask(c);
            }
          };

          writers.put(key, writer);
        }

        writer.write(new ImmutableBytesWritable(value.getRow()), value);
      }      
    };
  }

}