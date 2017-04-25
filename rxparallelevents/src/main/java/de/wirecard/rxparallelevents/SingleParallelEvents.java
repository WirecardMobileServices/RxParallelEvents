package de.wirecard.rxparallelevents;

import io.reactivex.Observer;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.subjects.Subject;

public class SingleParallelEvents<T> extends Single<T> {

    private Subject<Event> eventObservable;
    private Single<T> flowSingle;

    public SingleParallelEvents(Single<T> flowSingle, Subject<Event> eventObservable) {
        this.flowSingle = flowSingle;
        this.eventObservable = eventObservable;
    }

    @Override
    protected void subscribeActual(SingleObserver<? super T> observer) {
        flowSingle.subscribe(observer);
    }

    public Single<T> subscribeForEvents(Observer<Event> eventObservable) {
        if (eventObservable != null) {
            this.eventObservable.subscribeWith(eventObservable);
        }
        return flowSingle;
    }
}