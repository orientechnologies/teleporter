package com.orientechnologies.teleporter.test.rdbms.synch;

import com.orientechnologies.orient.output.OOutputStreamManager;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by gabriele on 11/08/17.
 */
public class SynchOutputStreamTest {

  private OOutputStreamManager  streamManager;
  private PrintStream           stream;
  private ByteArrayOutputStream baos;

  @Before
  public void init() {
    this.baos = new ByteArrayOutputStream();
    this.stream = new PrintStream(this.baos);
  }

  @Test
  public void test1() {

    this.streamManager = new OOutputStreamManager(2);

    Callable writer = new Callable<Void>() {

      public Void call() {
        try {

          for (int i = 1; i <= 50; i++) {
            streamManager.info("Message " + i + " from writer thread.\n");
          }
        }catch(Exception e) {

        }
        return null;
      }
    };

    Callable flusher = new Callable<Void>() {
      @Override
      public Void call() {

        System.out.println("Flushing started.");

        synchronized (streamManager) {
          try {
            // DO NOTHING
            Thread.sleep(5000);
          } catch(Exception e) {
            e.printStackTrace();
            fail();
          }
        }

        System.out.println("Flushing completed.");
        return null;
      }
    };

    List<Callable<Void>> threads = new ArrayList<Callable<Void>>();
    threads.add(writer);
    threads.add(flusher);
    final ExecutorService threadExecutors = Executors.newCachedThreadPool();

    try {
      List<Future<Void>> futures = threadExecutors.invokeAll(threads);

      for (Future<Void> future : futures) {
        future.get();
      }
    } catch(Exception e) {
      e.printStackTrace();
    }


  }

  @Test
  public void test2() {

    this.streamManager = new OOutputStreamManager(this.stream, 2);

    Callable writer = new Callable<Void>() {

      public Void call() {
        try {

          for (int i = 1; i <= 50; i++) {
            streamManager.info("Message " + i + " from writer thread.\n");
          }
        }catch(Exception e) {

        }
        return null;
      }
    };

    Callable flusher = new Callable<Void>() {
      @Override
      public Void call() {

        System.out.println("Flushing started.");

        synchronized (streamManager) {
          try {
            int initialBufferSize = baos.size();
            String flush = baos.toString();

            int finalBufferSize = baos.size();
            assertEquals(finalBufferSize - initialBufferSize, 0);

            // getting the baos empty
            baos.reset();
            assertEquals(baos.size(), 0);
          } catch(Exception e) {
            e.printStackTrace();
            fail();
          }
        }
        try {
          Thread.sleep(1000);
          assertTrue(baos.size() > 0);
        } catch(Exception e) {
          e.printStackTrace();
          fail();
        }

        System.out.println("Flushing completed.");
        return null;
      }
    };

    List<Callable<Void>> threads = new ArrayList<Callable<Void>>();
    threads.add(writer);
    threads.add(flusher);
    final ExecutorService threadExecutors = Executors.newCachedThreadPool();

    try {
      List<Future<Void>> futures = threadExecutors.invokeAll(threads);

      for (Future<Void> future : futures) {
        future.get();
      }
    } catch(Exception e) {
      e.printStackTrace();
    }


  }

}
