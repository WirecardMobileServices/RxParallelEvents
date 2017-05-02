package de.wirecard.rxparallel;

import com.jakewharton.rxrelay2.Relay;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Observer;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Function;

public class CompletableParallel<PARALLEL> extends Completable {

    private Completable mainCompletable;
    private Relay<PARALLEL> parallelRelay;

    private CompletableParallel(Completable mainCompletable, Relay<PARALLEL> parallelRelay) {
        if (mainCompletable == null)
            throw new NullPointerException("Main completable can not be null");
        this.mainCompletable = mainCompletable;
        this.parallelRelay = parallelRelay;
    }

    @Override
    protected void subscribeActual(CompletableObserver s) {
        mainCompletable.subscribe(s);
    }

    public Completable subscribeParallel(Observer<PARALLEL> parallelObserver) {
        if (this.parallelRelay != null && parallelObserver != null) {
            this.parallelRelay.subscribeWith(parallelObserver);
        }
        return mainCompletable;
    }

    public static <PARALLEL> Function<? super Completable, CompletableParallel<PARALLEL>> with(final Relay<PARALLEL> parallelSubject) {
        return new Function<Completable, CompletableParallel<PARALLEL>>() {
            @Override
            public CompletableParallel<PARALLEL> apply(@NonNull Completable completable) throws Exception {
                return new CompletableParallel<PARALLEL>(completable, parallelSubject);
            }
        };
    }
}
