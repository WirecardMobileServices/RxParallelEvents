package de.wirecard.rxparallel;

import com.jakewharton.rxrelay2.PublishRelay;
import com.jakewharton.rxrelay2.Relay;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import io.reactivex.Completable;
import io.reactivex.CompletableSource;
import io.reactivex.functions.Action;

import static org.hamcrest.core.Is.is;

public class CompletableParallelTest extends AbsRxTest {

    @Test
    public void completableParallelWithRelay() {
        final Relay<String> source = PublishRelay.create();
        Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event");
        consumer.subscribe(valueAssert);

        Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Completable started");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        })
                .to(CompletableParallel.with(source))
                .subscribeParallel(consumer)
                .test()
                .assertComplete();

        Assert.assertThat("event count", valueAssert.getCount(), is(1));
    }

    @Test
    public void completableParallelWithRelay_sameSource() {
        final Relay<String> source = PublishRelay.create();
        Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event", "event2");
        consumer.subscribe(valueAssert);

        Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Completable started");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        }).to(CompletableParallel.with(source))
                .subscribeParallel(consumer)
                .test()
                .assertComplete();

        Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Completable 2 started");
                System.out.println("Sending event2");
                source.accept("event2");
                return "Im Done";
            }
        }).to(CompletableParallel.with(source))
                .subscribeParallel(consumer)
                .test()
                .assertComplete();

        Assert.assertThat("event count", valueAssert.getCount(), is(2));
    }

    @Test
    public void completableParallelWithRelay_twoSources() {
        final Relay<String> source = PublishRelay.create();
        final Relay<String> source2 = PublishRelay.create();

        Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event", "event2");
        consumer.subscribe(valueAssert);

        Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Completable started");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        }).to(CompletableParallel.with(source))
                .subscribeParallel(consumer)
                .test()
                .assertComplete();

        Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Completable 2 started");
                System.out.println("Sending event2");
                source2.accept("event2");
                return "Im Done";
            }
        }).to(CompletableParallel.with(source2))
                .subscribeParallel(consumer)
                .test()
                .assertComplete();

        Assert.assertThat("event count", valueAssert.getCount(), is(2));
    }

    @Test
    public void completableParallelWithRelay_error() {
        final Relay<String> source = PublishRelay.create();
        Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event");
        consumer.subscribe(valueAssert);

        Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Completable started");
                System.out.println("Sending event");
                source.accept("event");
                return throwMe();
            }
        }).to(CompletableParallel.with(source))
                .subscribeParallel(consumer)
                .test()
                .assertError(RuntimeException.class);

        Assert.assertThat("event count", valueAssert.getCount(), is(1));
    }

    private String throwMe() {
        throw new RuntimeException("Completable failed");
    }

    @Test
    public void completableParallelWithRelay_sameSource_error() {
        final Relay<String> source = PublishRelay.create();
        Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event", "event2");
        consumer.subscribe(valueAssert);

        Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Completable started");
                System.out.println("Sending event");
                source.accept("event");
                throwMe();
                return null;
            }
        }).to(CompletableParallel.with(source))
                .subscribeParallel(consumer)
                .test()
                .assertError(RuntimeException.class);

        Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Completable 2 started");
                System.out.println("Sending event2");
                source.accept("event2");
                return "Im Done";
            }
        }).to(CompletableParallel.with(source))
                .subscribeParallel(consumer)
                .test()
                .assertComplete();

        Assert.assertThat("event count", valueAssert.getCount(), is(2));
    }

    @Test
    public void completableParallelWithRelay_multiParallelSub() {
        final Relay<String> source = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event", "event2");
        AbsRxTest.AsyncValueAssert<String> valueAssert2 = new AbsRxTest.AsyncValueAssert<String>("event", "event2");

        Relay<String> consumer = PublishRelay.create();
        consumer.subscribe(valueAssert);
        Relay<String> consumer2 = PublishRelay.create();
        consumer2.subscribe(valueAssert2);

        CompletableParallel<String> parallel = Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Completable started");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        }).to(CompletableParallel.with(source));
        parallel.subscribeParallel(consumer);
        parallel.subscribeParallel(consumer2);
        parallel.test()
                .assertComplete();

        Assert.assertThat("event count", valueAssert.getCount(), is(1));
        Assert.assertThat("event count", valueAssert2.getCount(), is(1));
    }

    @Test
    public void completableParallelWithRelay_multiParallelSub_sameSource() {
        final Relay<String> source = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event", "event2");
        AbsRxTest.AsyncValueAssert<String> valueAssert2 = new AbsRxTest.AsyncValueAssert<String>("event", "event2");

        Relay<String> consumer = PublishRelay.create();
        consumer.subscribe(valueAssert);
        Relay<String> consumer2 = PublishRelay.create();
        consumer2.subscribe(valueAssert2);

        CompletableParallel<String> parallel = Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Completable started");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        }).to(CompletableParallel.with(source));
        parallel.subscribeParallel(consumer);
        parallel.subscribeParallel(consumer2);
        parallel.test()
                .assertComplete();

        CompletableParallel<String> parallel2 = Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Completable started");
                System.out.println("Sending event");
                source.accept("event2");
                return "Im Done";
            }
        }).to(CompletableParallel.with(source));
        parallel2.subscribeParallel(consumer);
        parallel2.subscribeParallel(consumer2);
        parallel2.test()
                .assertComplete();

        Assert.assertThat("event count", valueAssert.getCount(), is(2));
        Assert.assertThat("event count", valueAssert2.getCount(), is(2));
    }

    @Test
    public void completableParallelWithoutSub() {
        final Relay<String> source = PublishRelay.create();

        Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Starting Completable");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        }).to(CompletableParallel.with(source))
                .test()
                .assertComplete();

    }

    @Test
    public void completableParallelWithoutMultipleEventInChain() {
        final Relay<String> source = PublishRelay.create();
        Relay<String> consumer = PublishRelay.create();
        final AbsRxTest.AsyncValueAssert<String> valueAssert = new AbsRxTest.AsyncValueAssert<String>("event", "event3");
        consumer.subscribe(valueAssert);

        Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Starting Completable");
                System.out.println("Sending event");
                source.accept("event");
                return "Im Done";
            }
        }).to(CompletableParallel.with(source))
                .subscribeParallel(consumer)
                .andThen(Completable.fromCallable(new Callable<String>() {
                    @Override
                    public String call() {
                        System.out.println("Starting next Completable");
                        System.out.println("Sending event2");
                        //won't be propagated because disposable has been already disposed
                        source.accept("event2");
                        return "Im Done";
                    }
                }))
                .test()
                .assertComplete();

        Completable.fromCallable(new Callable<String>() {
            @Override
            public String call() {
                System.out.println("Starting Completable");
                System.out.println("Sending event3");
                source.accept("event3");
                return "Im Done";
            }
        }).to(CompletableParallel.with(source))
                .subscribeParallel(consumer)
                .andThen(Completable.fromCallable(new Callable<String>() {
                    @Override
                    public String call() {
                        System.out.println("Starting next Completable");
                        System.out.println("Sending event4");
                        //won't be propagated because disposable has been already disposed
                        source.accept("event4");
                        return "Im Done";
                    }
                }))
                .test()
                .assertComplete();

        Assert.assertThat("event count", valueAssert.getCount(), is(2));
    }

    @Test
    public void completableAndThen() {
        final Relay<String> source = PublishRelay.create();
        Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AsyncValueAssert<String>("First completed", "Second completed", "Third completed");
        consumer.subscribe(valueAssert);

        Completable.complete()
                .doOnComplete(new Action() {
                    @Override
                    public void run() {
                        System.out.println("Sending first event");
                        source.accept("First completed");
                    }
                })
                .andThen(Completable.complete())
                .doOnComplete(new Action() {
                    @Override
                    public void run() {
                        System.out.println("Second first event");
                        source.accept("Second completed");
                    }
                })
                .andThen(Completable.complete().delay(1, TimeUnit.SECONDS))
                .doOnComplete(new Action() {
                    @Override
                    public void run() {
                        System.out.println("Third first event");
                        source.accept("Third completed");
                    }
                })
                .to(CompletableParallel.with(source))
                .subscribeParallel(consumer)
                .blockingAwait();

        Assert.assertThat("events count", valueAssert.getCount(), is(3));
    }

    @Test
    public void completebleAndThenNestedCompletable() {
        final Relay<String> source = PublishRelay.create();
        final Relay<String> innerSource = PublishRelay.create();
        final Relay<String> consumer = PublishRelay.create();
        AbsRxTest.AsyncValueAssert<String> valueAssert = new AsyncValueAssert<String>("First completed", "Second completed");
        consumer.subscribe(valueAssert);

        Completable.fromAction(new Action() {
            @Override
            public void run() {
                System.out.println("Starting completable");
                System.out.println("Sending first event");
                source.accept("First completed");
            }
        }).andThen(Completable.defer(new Callable<CompletableSource>() {
            @Override
            public CompletableSource call() {
                System.out.println("Starting completable2");
                return Completable.fromAction(new Action() {
                    @Override
                    public void run() {
                        System.out.println("Starting nested completable");
                        System.out.println("Sending second event");
                        innerSource.accept("Second completed");
                    }
                }).to(CompletableParallel.with(innerSource))
                        .subscribeParallel(consumer);

            }
        })).to(CompletableParallel.with(source))
                .subscribeParallel(consumer)
                .subscribe();

        Assert.assertThat("events count", valueAssert.getCount(), is(2));
    }
}
