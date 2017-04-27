package de.wirecard.rxparallel;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Function;
import io.reactivex.subjects.Subject;

public class SingleParallel<SINGLE, PARALLEL> extends Single<SINGLE> {

    private Subject<PARALLEL> eventObservable;
    private Single<SINGLE> flowSingle;

    public SingleParallel(Single<SINGLE> flowSingle, Subject<PARALLEL> eventObservable) {
        this.flowSingle = flowSingle;
        this.eventObservable = eventObservable;
    }

    @Override
    protected void subscribeActual(SingleObserver<? super SINGLE> observer) {
        flowSingle.subscribe(observer);
    }

    public Single<SINGLE> subscribeForEvents(Observer<PARALLEL> eventObservable) {
        if (this.eventObservable != null && eventObservable != null) {
            this.eventObservable.subscribeWith(eventObservable);
        }
        return flowSingle;
    }

    public static <SINGLE, PARALLEL> Function<? super Observable<SINGLE>, ObservableParallel<SINGLE, PARALLEL>> with(final Subject<PARALLEL> subject) {
        return new Function<Observable<SINGLE>, ObservableParallel<SINGLE, PARALLEL>>() {
            @Override
            public ObservableParallel<SINGLE, PARALLEL> apply(@NonNull Observable<SINGLE> singleObservable) throws Exception {
                return new ObservableParallel<SINGLE, PARALLEL>(singleObservable, subject);
            }
        };
    }
}