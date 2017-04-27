package de.wirecard.rxparallel;

import io.reactivex.Observer;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.subjects.Subject;

public class SingleParallel<SINGLE, PARALLEL> extends Single<SINGLE> {

    private Subject<PARALLEL> eventObservable;
    private Single<SINGLE> flowSingle;

    public SingleParallel(Single<SINGLE> flowSingle, Subject<PARALLEL> eventObservable) {
        this.flowSingle = flowSingle;
        this.eventObservable = eventObservable;
    }

    @Override
    protected void subscribeActual(SingleObserver<? super SINGLE> observer) {
        flowSingle.subscribe(observer);
    }

    public Single<SINGLE> subscribeForEvents(Observer<PARALLEL> eventObservable) {
        if (this.eventObservable != null && eventObservable != null) {
            this.eventObservable.subscribeWith(eventObservable);
        }
        return flowSingle;
    }
}