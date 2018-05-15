package de.wirecard.rxparallel;

import com.jakewharton.rxrelay2.Relay;

import de.wirecard.rxparallel.util.SafeDisposeAction;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.functions.Function;

public class ObservableParallel<OBSERVABLE, PARALLEL> extends Observable<OBSERVABLE> {

    private Observable<OBSERVABLE> mainObservable;
    private Relay<PARALLEL> parallelRelay;
    private CompositeDisposable relayDisposable;

    private ObservableParallel(Observable<OBSERVABLE> mainObservable, Relay<PARALLEL> parallelRelay) {
        if (mainObservable == null)
            throw new NullPointerException("Main observable can not be null");
        relayDisposable = new CompositeDisposable();
        this.mainObservable = mainObservable.doAfterTerminate(SafeDisposeAction.createAction(relayDisposable));
        this.parallelRelay = parallelRelay;
    }

    @Override
    protected void subscribeActual(Observer<? super OBSERVABLE> observer) {
        mainObservable.subscribe(observer);
    }

    public Observable<OBSERVABLE> subscribeParallel(Observer<PARALLEL> parallelObserver) {
        if (this.parallelRelay != null && parallelObserver != null) {
            this.parallelRelay.subscribeWith(parallelObserver);
        }
        return mainObservable;
    }

    public Observable<OBSERVABLE> subscribeParallel(Relay<PARALLEL> parallelRelay) {
        if (this.parallelRelay != null && parallelRelay != null) {
            relayDisposable.add(this.parallelRelay.subscribe(parallelRelay));
        }
        return mainObservable;
    }

    public static <OBSERVABLE, PARALLEL> Function<? super Observable<OBSERVABLE>, ObservableParallel<OBSERVABLE, PARALLEL>> with(final Relay<PARALLEL> parallelRelay) {
        return new Function<Observable<OBSERVABLE>, ObservableParallel<OBSERVABLE, PARALLEL>>() {
            @Override
            public ObservableParallel<OBSERVABLE, PARALLEL> apply(@NonNull Observable<OBSERVABLE> observable) throws Exception {
                return new ObservableParallel<OBSERVABLE, PARALLEL>(observable, parallelRelay);
            }
        };
    }
}