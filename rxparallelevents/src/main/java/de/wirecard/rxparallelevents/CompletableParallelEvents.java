package de.wirecard.rxparallelevents;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Observer;
import io.reactivex.subjects.Subject;

public class CompletableParallelEvents extends Completable {

    private Subject<Event> eventObservable;
    private Completable flowCompletable;

    public CompletableParallelEvents(Completable flowCompletable, Subject<Event> eventObservable) {
        this.flowCompletable = flowCompletable;
        this.eventObservable = eventObservable;
    }

    @Override
    protected void subscribeActual(CompletableObserver s) {
        flowCompletable.subscribe(s);
    }

    public Completable subscribeForEvents(Observer<Event> eventObservable) {
        if (eventObservable != null) {
            this.eventObservable.subscribeWith(eventObservable);
        }
        return flowCompletable;
    }
}
