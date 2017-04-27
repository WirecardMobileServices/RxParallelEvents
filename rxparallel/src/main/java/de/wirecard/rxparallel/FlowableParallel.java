package de.wirecard.rxparallel;

import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;
import io.reactivex.Observer;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Function;
import io.reactivex.subjects.Subject;

public class FlowableParallel<FLOWABLE, PARALLEL> extends Flowable<FLOWABLE> {

    private Flowable<FLOWABLE> mainFlowable;
    private Subject<PARALLEL> parallelSubject;

    private FlowableParallel(Flowable<FLOWABLE> mainFlowable, Subject<PARALLEL> parallelSubject) {
        if(mainFlowable == null)
            throw new NullPointerException("Main flowable can not be null");
        this.mainFlowable = mainFlowable;
        this.parallelSubject = parallelSubject;
    }

    @Override
    protected void subscribeActual(Subscriber<? super FLOWABLE> observer) {
        mainFlowable.subscribe(observer);
    }

    public Flowable<FLOWABLE> subscribeParallel(Observer<PARALLEL> parallelObserver) {
        if (this.parallelSubject != null && parallelObserver != null) {
            this.parallelSubject.subscribeWith(parallelObserver);
        }
        return mainFlowable;
    }

    public static <FLOWABLE, PARALLEL> Function<? super Flowable<FLOWABLE>, FlowableParallel<FLOWABLE, PARALLEL>> with(final Subject<PARALLEL> parallelSubject) {
        return new Function<Flowable<FLOWABLE>, FlowableParallel<FLOWABLE, PARALLEL>>() {
            @Override
            public FlowableParallel<FLOWABLE, PARALLEL> apply(@NonNull Flowable<FLOWABLE> flowable) throws Exception {
                return new FlowableParallel<FLOWABLE, PARALLEL>(flowable, parallelSubject);
            }
        };
    }
}
