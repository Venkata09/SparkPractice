package concurrency;

import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Created by vdokku on 1/13/2018.
 */
public class CyclicBarrierDemo {

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newCachedThreadPool();
        final Random randomNumber = new Random();

        final CyclicBarrier cyclicBarrierRun = new CyclicBarrier(4, () -> System.out.println("Hello I am learning Cyclic Barrier"));

        for (int i = 0; i < 4; i++) {
            executorService.execute(() -> {
                try {
                    Thread.sleep(randomNumber.nextInt(1000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                System.out.println(Thread.currentThread().getName() + "I am printing from the Cyclic Barrier :>>> ");

                try {
                    cyclicBarrierRun.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }

            });
        }

        executorService.shutdown();


    }
}
