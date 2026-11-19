#!/usr/bin/env python3
"""Bounded, read-only wall/monotonic clock probe; never sets time or changes NTP."""
import argparse
import datetime as dt
import json
import time

from provision import ROOT


def observe(seconds, stop=None):
    began=time.monotonic_ns()
    prior=None
    samples=0
    backward=0
    mono_backward=0
    events=[]
    while time.monotonic_ns()-began<seconds*1_000_000_000 and not (stop and stop.is_set()):
        left=time.monotonic_ns()
        wall=time.time_ns()
        right=time.monotonic_ns()
        # Clock-read scheduling is bounded by [left,right], not mistaken for
        # a correction. Disjoint wall-minus-monotonic intervals prove a step.
        lower,upper=wall-right,wall-left
        if prior:
            correction=prior[0]-upper
            if right<prior[2]:
                mono_backward+=1
            if correction>100_000:
                backward+=1
                if len(events)<256:
                    event=dict(elapsed_seconds=(right-began)/1e9,backward_offset_at_least_ns=correction,
                               wall_delta_ns=wall-prior[3],monotonic_delta_ns=right-prior[2])
                    events.append(event)
                    print(json.dumps(event),flush=True)
        prior=(lower,upper,right,wall)
        samples+=1
        time.sleep(.002)
    return dict(requested_seconds=seconds,observed_seconds=(time.monotonic_ns()-began)/1e9,
                samples=samples,backward_wall_offset_changes=backward,
                monotonic_backwards=mono_backward,events=events,
                scope='Read-only bounded local clock observation, not timestamp-aligned to an earlier HTTP request.')


def save(result):
    stamp=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    path=ROOT/'.e2e'/('p2-clock-'+stamp+'.json')
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(result,indent=2))
    print(json.dumps(result),flush=True)
    print('Evidence:',path.relative_to(ROOT),flush=True)
    return path


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds',type=int,default=45,choices=range(1,61))
    args=parser.parse_args()
    save(observe(args.seconds))


if __name__=='__main__':
    main()
