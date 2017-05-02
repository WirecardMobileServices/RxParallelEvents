package de.wirecard.rxparallel;

import com.jakewharton.rxrelay2.Relay;

import io.reactivex.Observer;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Function;

public class SingleParallel<SINGLE, PARALLEL> extends Single<SINGLE> {

    private Single<SINGLE> mainSingle;
    private Relay<PARALLEL> parallelRelay;

    private SingleParallel(Single<SINGLE> mainSingle, Relay<PARALLEL> parallelRelay) {
        if (mainSingle == null)
            throw new NullPointerException("Main single can not be null");
        this.mainSingle = mainSingle;
        this.parallelRelay = parallelRelay;
    }

    @Override
    protected void subscribeActual(SingleObserver<? super SINGLE> observer) {
        mainSingle.subscribe(observer);
    }

    public Single<SINGLE> subscribeParallel(Observer<PARALLEL> parallelObserver) {
        if (this.parallelRelay != null && parallelObserver != null) {
            this.parallelRelay.subscribeWith(parallelObserver);
        }
        return mainSingle;
    }

    public static <SINGLE, PARALLEL> Function<? super Single<SINGLE>, SingleParallel<SINGLE, PARALLEL>> with(final Relay<PARALLEL> parallelRelay) {
        return new Function<Single<SINGLE>, SingleParallel<SINGLE, PARALLEL>>() {
            @Override
            public SingleParallel<SINGLE, PARALLEL> apply(@NonNull Single<SINGLE> single) throws Exception {
                return new SingleParallel<SINGLE, PARALLEL>(single, parallelRelay);
            }
        };
    }
}