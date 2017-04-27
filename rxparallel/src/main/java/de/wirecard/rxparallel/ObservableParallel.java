package de.wirecard.rxparallel;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Function;
import io.reactivex.subjects.Subject;

public class ObservableParallel<OBSERVABLE, PARALLEL> extends Observable<OBSERVABLE> {

    private Observable<OBSERVABLE> mainObservable;
    private Subject<PARALLEL> parallelSubject;

    public ObservableParallel(Observable<OBSERVABLE> mainObservable, Subject<PARALLEL> parallelSubject) {
        this.mainObservable = mainObservable;
        this.parallelSubject = parallelSubject;
    }

    @Override
    protected void subscribeActual(Observer<? super OBSERVABLE> observer) {
        mainObservable.subscribe(observer);
    }

    public Observable<OBSERVABLE> subscribeParallel(Observer<PARALLEL> parallelObserver) {
        if (this.parallelSubject != null && parallelObserver != null) {
            this.parallelSubject.subscribeWith(parallelObserver);
        }
        return mainObservable;
    }

    public static <OBSERVABLE, PARALLEL> Function<? super Observable<OBSERVABLE>, ObservableParallel<OBSERVABLE, PARALLEL>> with(final Subject<PARALLEL> parallelSubject) {
        return new Function<Observable<OBSERVABLE>, ObservableParallel<OBSERVABLE, PARALLEL>>() {
            @Override
            public ObservableParallel<OBSERVABLE, PARALLEL> apply(@NonNull Observable<OBSERVABLE> observable) throws Exception {
                return new ObservableParallel<OBSERVABLE, PARALLEL>(observable, parallelSubject);
            }
        };
    }
}