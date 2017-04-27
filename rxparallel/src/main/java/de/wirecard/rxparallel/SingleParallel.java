package de.wirecard.rxparallel;

import io.reactivex.Observer;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Function;
import io.reactivex.subjects.Subject;

public class SingleParallel<SINGLE, PARALLEL> extends Single<SINGLE> {

    private Single<SINGLE> mainSingle;
    private Subject<PARALLEL> parallelSubject;

    public SingleParallel(Single<SINGLE> mainSingle, Subject<PARALLEL> parallelSubject) {
        this.mainSingle = mainSingle;
        this.parallelSubject = parallelSubject;
    }

    @Override
    protected void subscribeActual(SingleObserver<? super SINGLE> observer) {
        mainSingle.subscribe(observer);
    }

    public Single<SINGLE> subscribeParallel(Observer<PARALLEL> parallelObserver) {
        if (this.parallelSubject != null && parallelObserver != null) {
            this.parallelSubject.subscribeWith(parallelObserver);
        }
        return mainSingle;
    }

    public static <SINGLE, PARALLEL> Function<? super Single<SINGLE>, SingleParallel<SINGLE, PARALLEL>> with(final Subject<PARALLEL> parallelSubject) {
        return new Function<Single<SINGLE>, SingleParallel<SINGLE, PARALLEL>>() {
            @Override
            public SingleParallel<SINGLE, PARALLEL> apply(@NonNull Single<SINGLE> single) throws Exception {
                return new SingleParallel<SINGLE, PARALLEL>(single, parallelSubject);
            }
        };
    }
}