package de.wirecard.rxparallel;

import com.jakewharton.rxrelay2.PublishRelay;
import com.jakewharton.rxrelay2.Relay;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import io.reactivex.Maybe;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;

import static org.hamcrest.core.Is.is;

public class MaybeFlowableTest {
    @Test
    public void maybeParallelWithRelay() {
        final Relay<String> source = PublishRelay.create();
        Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event");
        consumer.subscribe(valueAssert);

        Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Maybe started");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        })
                .to(MaybeParallel.<String, String>with(source))
                .subscribeParallel(consumer)
                .test()
                .assertValue("Im Done");

        Assert.assertThat("event count", valueAssert.getCount(), is(1));
    }

    @Test
    public void maybeParallelWithRelay_sameSource() {
        final Relay<String> source = PublishRelay.create();
        Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event", "event2");
        consumer.subscribe(valueAssert);

        Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Maybe started");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        }).to(MaybeParallel.<String, String>with(source))
                .subscribeParallel(consumer)
                .test()
                .assertValue("Im Done");

        Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Maybe 2 started");
                System.out.println("Sending event2");
                source.accept("event2");
                return "Im Done";
            }
        }).to(MaybeParallel.<String, String>with(source))
                .subscribeParallel(consumer)
                .test()
                .assertValue("Im Done");

        Assert.assertThat("event count", valueAssert.getCount(), is(2));
    }

    @Test
    public void maybeParallelWithRelay_twoSources() {
        final Relay<String> source = PublishRelay.create();
        final Relay<String> source2 = PublishRelay.create();

        Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event", "event2");
        consumer.subscribe(valueAssert);

        Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Maybe started");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        }).to(MaybeParallel.<String, String>with(source))
                .subscribeParallel(consumer)
                .test()
                .assertValue("Im Done");

        Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Maybe 2 started");
                System.out.println("Sending event2");
                source2.accept("event2");
                return "Im Done";
            }
        }).to(MaybeParallel.<String, String>with(source2))
                .subscribeParallel(consumer)
                .test()
                .assertValue("Im Done");

        Assert.assertThat("event count", valueAssert.getCount(), is(2));
    }

    @Test
    public void maybeParallelWithRelay_error() {
        final Relay<String> source = PublishRelay.create();
        Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event");
        consumer.subscribe(valueAssert);

        Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Maybe started");
                System.out.println("Sending event");
                source.accept("event");
                return throwMe();
            }
        }).to(MaybeParallel.<String, String>with(source))
                .subscribeParallel(consumer)
                .test()
                .assertError(RuntimeException.class);

        Assert.assertThat("event count", valueAssert.getCount(), is(1));
    }

    private String throwMe() {
        throw new RuntimeException("Maybe failed");
    }

    @Test
    public void maybeParallelWithRelay_sameSource_error() {
        final Relay<String> source = PublishRelay.create();
        Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event", "event2");
        consumer.subscribe(valueAssert);

        Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Maybe started");
                System.out.println("Sending event");
                source.accept("event");
                throwMe();
                return null;
            }
        }).to(MaybeParallel.<String, String>with(source))
                .subscribeParallel(consumer)
                .test()
                .assertError(RuntimeException.class);

        Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Maybe 2 started");
                System.out.println("Sending event2");
                source.accept("event2");
                return "Im Done";
            }
        }).to(MaybeParallel.<String, String>with(source))
                .subscribeParallel(consumer)
                .test()
                .assertValue("Im Done");

        Assert.assertThat("event count", valueAssert.getCount(), is(2));
    }

    @Test
    public void maybeParallelWithRelay_multiParallelSub() {
        final Relay<String> source = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event", "event2");
        AbsRxTest.AsyncValueAssert<String> valueAssert2 = new AbsRxTest.AsyncValueAssert<String>("event", "event2");

        Relay<String> consumer = PublishRelay.create();
        consumer.subscribe(valueAssert);
        Relay<String> consumer2 = PublishRelay.create();
        consumer2.subscribe(valueAssert2);

        MaybeParallel<String, String> parallel = Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Maybe started");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        }).to(MaybeParallel.<String, String>with(source));
        parallel.subscribeParallel(consumer);
        parallel.subscribeParallel(consumer2);
        parallel.test()
                .assertComplete();

        Assert.assertThat("event count", valueAssert.getCount(), is(1));
        Assert.assertThat("event count", valueAssert2.getCount(), is(1));
    }

    @Test
    public void maybeParallelWithRelay_multiParallelSub_sameSource() {
        final Relay<String> source = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event", "event2");
        AbsRxTest.AsyncValueAssert<String> valueAssert2 = new AbsRxTest.AsyncValueAssert<String>("event", "event2");

        Relay<String> consumer = PublishRelay.create();
        consumer.subscribe(valueAssert);
        Relay<String> consumer2 = PublishRelay.create();
        consumer2.subscribe(valueAssert2);

        MaybeParallel<String, String> parallel = Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Maybe started");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        }).to(MaybeParallel.<String, String>with(source));
        parallel.subscribeParallel(consumer);
        parallel.subscribeParallel(consumer2);
        parallel.test()
                .assertComplete();

        MaybeParallel<String, String> parallel2 = Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Maybe started");
                System.out.println("Sending event");
                source.accept("event2");
                return "Im Done";
            }
        }).to(MaybeParallel.<String, String>with(source));
        parallel2.subscribeParallel(consumer);
        parallel2.subscribeParallel(consumer2);
        parallel2.test()
                .assertComplete();

        Assert.assertThat("event count", valueAssert.getCount(), is(2));
        Assert.assertThat("event count", valueAssert2.getCount(), is(2));
    }

    @Test
    public void maybeParallelWithoutSub() {
        final Relay<String> source = PublishRelay.create();

        Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Starting maybe");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        }).to(MaybeParallel.<String, String>with(source))
                .map(new Function<String, String>() {
                    @Override
                    public String apply(String s) {
                        return s.toUpperCase();
                    }
                }).test()
                .assertComplete()
                .assertValue("IM DONE");

    }

    @Test
    public void maybeParallelWithoutMultipleEventInChain() {
        final Relay<String> source = PublishRelay.create();
        Relay<String> consumer = PublishRelay.create();
        final AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event", "event2", "event3", "event4");
        consumer.subscribe(valueAssert);

        Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Starting maybe");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        }).to(MaybeParallel.<String, String>with(source))
                .subscribeParallel(consumer)
                .map(new Function<String, String>() {
                    @Override
                    public String apply(String s) {
                        System.out.println("Sending event2");
                        source.accept("event2");
                        return s.toUpperCase();
                    }
                }).test()
                .assertComplete()
                .assertValue("IM DONE");

        Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Starting maybe");
                System.out.println("Sending event3");
                source.accept("event3");
                return "Im Done";
            }
        }).to(MaybeParallel.<String, String>with(source))
                .subscribeParallel(consumer)
                .map(new Function<String, String>() {
                    @Override
                    public String apply(String s) {
                        System.out.println("Sending event4");
                        source.accept("event4");
                        return s.toUpperCase();
                    }
                }).test()
                .assertComplete()
                .assertValue("IM DONE");

        Assert.assertThat("event count", valueAssert.getCount(), is(4));
    }

    @Test
    public void maybeParallelDispose() throws InterruptedException {
        final Relay<String> source = PublishRelay.create();
        final Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("2", "1");
        consumer.subscribe(valueAssert);

        Disposable d = Maybe.fromCallable(new Callable<String>() {
            @Override
            public String call() throws Exception {
                source.accept("2");
                TimeUnit.SECONDS.sleep(3);
                return "Im Done";
            }
        })
                .to(MaybeParallel.<String, String>with(source))
                .subscribeParallel(consumer)
                .subscribeOn(Schedulers.single())
                .subscribe(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        //swallow
                    }
                }, new Consumer<Throwable>() {
                    @Override
                    public void accept(Throwable s) {
                        //swallow
                    }
                });
        TimeUnit.SECONDS.sleep(1);
        d.dispose();

        Maybe.just(1)
                .doOnSuccess(new Consumer<Integer>() {
                    @Override
                    public void accept(Integer integer) {
                        consumer.accept(String.valueOf(integer));
                    }
                })
                .to(MaybeParallel.<Integer, String>with(source))
                .subscribeParallel(consumer)
                .test()
                .assertValue(1);

        Assert.assertThat("event count", valueAssert.getCount(), is(2));
    }

    @Test
    public void maybeParallelEventInSubscribe() {
        final Relay<String> source = PublishRelay.create();
        final Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("Just", "Just do it!");
        consumer.subscribe(valueAssert);

        Maybe.just("Just")
                .doOnSuccess(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        source.accept(s);
                    }
                }).map(new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s.concat(" do it!");
            }
        }).to(MaybeParallel.<String, String>with(source))
                .subscribeParallel(consumer)
                .subscribe(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        source.accept(s);
                    }
                });

        Assert.assertThat("event count", valueAssert.getCount(), is(2));
    }

    @Test
    public void maybeWithoutEmission() {
        final Relay<String> source = PublishRelay.create();
        final Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("First");
        consumer.subscribe(valueAssert);

        Maybe.fromAction(new Action() {
            @Override
            public void run() {
                System.out.println("Some maybe action");
                System.out.println("Sending first event");
                source.accept("First");
            }
        }).to(MaybeParallel.with(source))
                .subscribeParallel(consumer)
                .subscribe();

        Assert.assertThat("event count", valueAssert.getCount(), is(1));
    }
}
