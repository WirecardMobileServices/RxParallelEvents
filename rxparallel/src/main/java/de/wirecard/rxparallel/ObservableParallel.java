package de.wirecard.rxparallel;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.subjects.Subject;

public class ObservableParallel<OBSERVABLE, PARALLEL> extends Observable<OBSERVABLE> {

    private Subject<PARALLEL> eventObservable;
    private Observable<OBSERVABLE> flowObservable;

    public ObservableParallel(Observable<OBSERVABLE> flowObservable, Subject<PARALLEL> eventObservable) {
        this.flowObservable = flowObservable;
        this.eventObservable = eventObservable;
    }

    @Override
    protected void subscribeActual(Observer<? super OBSERVABLE> observer) {
        flowObservable.subscribe(observer);
    }

    public Observable<OBSERVABLE> subscribeForEvents(Observer<PARALLEL> eventObservable) {
        if (this.eventObservable != null && eventObservable != null) {
            this.eventObservable.subscribeWith(eventObservable);
        }
        return flowObservable;
    }
}