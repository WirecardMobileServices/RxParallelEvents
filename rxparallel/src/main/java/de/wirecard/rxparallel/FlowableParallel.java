package de.wirecard.rxparallel;

import com.jakewharton.rxrelay2.Relay;

import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;
import io.reactivex.Observer;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Function;

public class FlowableParallel<FLOWABLE, PARALLEL> extends Flowable<FLOWABLE> {

    private Flowable<FLOWABLE> mainFlowable;
    private Relay<PARALLEL> parallelRelay;

    private FlowableParallel(Flowable<FLOWABLE> mainFlowable, Relay<PARALLEL> parallelRelay) {
        if (mainFlowable == null)
            throw new NullPointerException("Main flowable can not be null");
        this.mainFlowable = mainFlowable;
        this.parallelRelay = parallelRelay;
    }

    @Override
    protected void subscribeActual(Subscriber<? super FLOWABLE> observer) {
        mainFlowable.subscribe(observer);
    }

    public Flowable<FLOWABLE> subscribeParallel(Observer<PARALLEL> parallelObserver) {
        if (this.parallelRelay != null && parallelObserver != null) {
            this.parallelRelay.subscribeWith(parallelObserver);
        }
        return mainFlowable;
    }

    public static <FLOWABLE, PARALLEL> Function<? super Flowable<FLOWABLE>, FlowableParallel<FLOWABLE, PARALLEL>> with(final Relay<PARALLEL> parallelRelay) {
        return new Function<Flowable<FLOWABLE>, FlowableParallel<FLOWABLE, PARALLEL>>() {
            @Override
            public FlowableParallel<FLOWABLE, PARALLEL> apply(@NonNull Flowable<FLOWABLE> flowable) throws Exception {
                return new FlowableParallel<FLOWABLE, PARALLEL>(flowable, parallelRelay);
            }
        };
    }
}
