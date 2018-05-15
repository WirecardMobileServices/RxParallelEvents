package de.wirecard.rxparallel;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import io.reactivex.functions.Consumer;
import io.reactivex.plugins.RxJavaPlugins;

import static org.hamcrest.core.Is.is;

public class AbsRxTest {
    private List<Throwable> uncaughtExceptions;

    @Before
    public void catchExceptions() {
        uncaughtExceptions = new ArrayList<Throwable>();
        RxJavaPlugins.reset();
        RxJavaPlugins.setErrorHandler(new Consumer<Throwable>() {
            @Override
            public void accept(Throwable throwable) throws Exception {
                uncaughtExceptions.add(throwable);
            }
        });
    }

    @After
    public void checkExceptions() {
        if (uncaughtExceptions != null && !uncaughtExceptions.isEmpty()) {
            Assert.fail(uncaughtExceptions.get(0).getMessage());
//            uncaughtExceptions.get(0).printStackTrace();
        }
    }

    protected static class AsyncValueAssert<T> implements Consumer<T> {
        int count = 0;
        T[] values;

        public AsyncValueAssert(T... values) {
            this.values = values;
        }

        @Override
        public void accept(T s) throws Exception {
            System.out.println("Accept: " + s);
            Assert.assertThat(s, is(values[count++]));
        }

        public int getCount() {
            return count;
        }

        public void clear() {
            count = 0;
        }
    }
}
