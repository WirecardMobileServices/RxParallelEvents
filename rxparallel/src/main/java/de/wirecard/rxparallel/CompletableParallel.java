package de.wirecard.rxparallel;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Observer;
import io.reactivex.subjects.Subject;

public class CompletableParallel<PARALLEL> extends Completable {

    private Subject<PARALLEL> eventObservable;
    private Completable flowCompletable;

    public CompletableParallel(Completable flowCompletable, Subject<PARALLEL> eventObservable) {
        this.flowCompletable = flowCompletable;
        this.eventObservable = eventObservable;
    }

    @Override
    protected void subscribeActual(CompletableObserver s) {
        flowCompletable.subscribe(s);
    }

    public Completable subscribeForEvents(Observer<PARALLEL> eventObservable) {
        if (this.eventObservable != null && eventObservable != null) {
            this.eventObservable.subscribeWith(eventObservable);
        }
        return flowCompletable;
    }
}
