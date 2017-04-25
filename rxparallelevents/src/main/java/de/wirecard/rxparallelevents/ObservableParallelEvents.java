package de.wirecard.rxparallelevents;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.subjects.Subject;

public class ObservableParallelEvents<T> extends Observable<T> {

    private Subject<Event> eventObservable;
    private Observable<T> flowObservable;

    public ObservableParallelEvents(Observable<T> flowObservable, Subject<Event> eventObservable) {
        this.flowObservable = flowObservable;
        this.eventObservable = eventObservable;
    }

    @Override
    protected void subscribeActual(Observer<? super T> observer) {
        flowObservable.subscribe(observer);
    }

    public Observable<T> subscribeForEvents(Observer<Event> eventObservable) {
        if (eventObservable != null) {
            this.eventObservable.subscribeWith(eventObservable);
        }
        return flowObservable;
    }
}