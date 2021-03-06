package de.wirecard.rxparallel;

import com.jakewharton.rxrelay2.Relay;

import de.wirecard.rxparallel.util.SafeDisposeAction;
import io.reactivex.Observer;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.functions.Function;

public class SingleParallel<SINGLE, PARALLEL> extends Single<SINGLE> {

    private Single<SINGLE> mainSingle;
    private Relay<PARALLEL> parallelRelay;
    private CompositeDisposable relayDisposable;

    private SingleParallel(Single<SINGLE> mainSingle, Relay<PARALLEL> parallelRelay) {
        if (mainSingle == null)
            throw new NullPointerException("Main single can not be null");
        relayDisposable = new CompositeDisposable();
        this.mainSingle = mainSingle.doAfterTerminate(SafeDisposeAction.createAction(relayDisposable));
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

    public Single<SINGLE> subscribeParallel(Relay<PARALLEL> parallelRelay) {
        if (this.parallelRelay != null && parallelRelay != null) {
            relayDisposable.add(this.parallelRelay.subscribe(parallelRelay));
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