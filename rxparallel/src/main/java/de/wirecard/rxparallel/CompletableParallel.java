package de.wirecard.rxparallel;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Observer;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Function;
import io.reactivex.subjects.Subject;

public class CompletableParallel<PARALLEL> extends Completable {

    private Completable mainCompletable;
    private Subject<PARALLEL> parallelSubject;

    private CompletableParallel(Completable mainCompletable, Subject<PARALLEL> parallelSubject) {
        if(mainCompletable == null)
            throw new NullPointerException("Main completable can not be null");
        this.mainCompletable = mainCompletable;
        this.parallelSubject = parallelSubject;
    }

    @Override
    protected void subscribeActual(CompletableObserver s) {
        mainCompletable.subscribe(s);
    }

    public Completable subscribeParallel(Observer<PARALLEL> parallelObserver) {
        if (this.parallelSubject != null && parallelObserver != null) {
            this.parallelSubject.subscribeWith(parallelObserver);
        }
        return mainCompletable;
    }

    public static <PARALLEL> Function<? super Completable, CompletableParallel<PARALLEL>> with(final Subject<PARALLEL> parallelSubject) {
        return new Function<Completable, CompletableParallel<PARALLEL>>() {
            @Override
            public CompletableParallel<PARALLEL> apply(@NonNull Completable completable) throws Exception {
                return new CompletableParallel<PARALLEL>(completable, parallelSubject);
            }
        };
    }
}
