import logging
from datetime import timedelta

import azure.durable_functions as df
from fnc.utils import str_to_utc_datetime


def _pull_detection_history(
    context: df.DurableOrchestrationContext,
    event_type: str,
    history: dict,
    retrieved: list,
    failing: list
):
    end_date = str_to_utc_datetime(history.get("end_date_str"))
    checkpoint = str_to_utc_datetime(history.get("checkpoint"))

    if checkpoint < end_date:
        logging.info(f"Start fetching history for {event_type}")
        try:
            next_checkpoint = yield context.call_activity(
                "FetchAndSendDetectionsHistory",
                {"event_type": event_type, "history": history},
            )

            history["checkpoint"] = next_checkpoint
            retrieved.append(event_type)
        except Exception as ex:
            logging.error(
                f"Error when fetching Detections history. start_date: {checkpoint}, error: {ex}"
            )
            failing.append(event_type)

        return history, False

    retrieved.append(event_type)
    return history, True


def _pull_event_history(
    context: df.DurableOrchestrationContext,
    event_type: str,
    history: dict,
    attempt: int,
    retrieved: list,
    failing: list
):
    start_date = str_to_utc_datetime(history["start_date"])
    end_date = str_to_utc_datetime(history["end_date"])
    if start_date < end_date and attempt <= 3:
        logging.info(f"Start fetching history for {event_type}")
        try:
            next_history = yield context.call_activity(
                "FetchAndSendEventsHistory",
                {"history": history, "event_type": event_type},
            )

            attempt = 0
            retrieved.append(event_type)
        except Exception as ex:
            logging.error(
                f"Error when fetching events history event_type: {event_type}, start_date: {start_date}, end_date: {end_date} error: {ex}"
            )
            attempt += 1
            if attempt <= 3:
                logging.info(f"Retrying attempt {attempt}.")
            else:
                failing.append(event_type)
        return next_history, attempt, False

    if attempt > 3:
        failing.append(event_type)
    else:
        retrieved.append(event_type)
    return history, 0, True


def pull_history(
    context: df.DurableOrchestrationContext,
    args: dict
):
    event_types: dict = args.get("event_types")

    logging.info("SingletonEternalOrchestrator: Polling History")

    if not event_types:
        return

    failing_history = args.get("failing_history", [])
    retrieved_history = args.get("retrieved_history", [])
    for event_type, event_type_args in event_types.items():
        if event_type in failing_history or event_type in retrieved_history:
            continue

        # Retrieving Detections history by checkpoint
        if event_type == "detections":
            history = event_type_args["history_detections"]

            history, is_done = _pull_detection_history(
                context=context,
                event_type=event_type,
                history=history,
                retrieved=retrieved_history,
                failing=failing_history
            )

            event_types[event_type]["history_detections"] = history
        # Retrieving events history for each event_type hour by hour
        else:
            attempt = event_type_args.get("attempt", 0)
            history = event_type_args["history_events"]

            history, is_done = _pull_event_history(
                context=context,
                event_type=event_type,
                history=history,
                attempt=attempt,
                retrieved=retrieved_history,
                failing=failing_history
            )

            event_types[event_type]["history_events"] = history
            event_types[event_type]["attempt"] = attempt

        args["retrieved_history"] = retrieved_history
        args["failing_history"] = failing_history
        args["event_types"] = event_types

        if is_done:
            continue
        else:
            return False
    return True


def pull_recent_detections(
    context: df.DurableOrchestrationContext,
    event_type: str,
    checkpoint: str,
    retrieved: list,
    failing: list
):
    logging.info(f"Start fetching most recent data for {event_type}")
    try:
        next_checkpoint = yield context.call_activity(
            "FetchAndSendDetections",
            {"event_type": event_type, "checkpoint": checkpoint},
        )
        retrieved.append(event_type)
    except Exception as ex:
        logging.error(
            f"Error when fetching Detections by checkpoints, checkpoint: {checkpoint} error: {ex}"
        )
        failing.append(event_type)

    return next_checkpoint


def pull_recent_events(
    context: df.DurableOrchestrationContext,
    event_type: str,
    checkpoint: str,
    attempt: int,
    retrieved: list,
    failing: list
):
    logging.info(f"Start fetching most recent data for {event_type}")
    try:
        next_checkpoint = yield context.call_activity(
            "FetchAndSendEvents",
            {"checkpoint": checkpoint, "event_type": event_type},
        )
        retrieved.append(event_type)
    except Exception as ex:
        logging.error(
            f"Error when fetching events by checkpoints with event_type: {event_type}, checkpoint: {checkpoint} error: {ex}"
        )
        attempt += 1
        if attempt <= 3:
            logging.info(f"Retrying attempt {attempt}.")
        else:
            failing.append(event_type)

    return next_checkpoint, attempt


def pull_recent(
    context: df.DurableOrchestrationContext,
    args: dict
):
    event_types: dict = args.get("event_types")
    failing = args.get("failing", [])
    retrieved = args.get("retrieved", [])

    for event_type, event_type_args in event_types.items():
        attempt = event_type_args.get("attempt", 0)
        if event_type in failing or event_type in retrieved or attempt > 3:
            continue

        checkpoint = event_type_args["checkpoint"]
        # Retrieving piece of a day for detections
        if event_type == "detections":
            next_checkpoint = pull_recent_detections(
                context=context,
                event_type=event_type,
                checkpoint=checkpoint,
                retrieved=retrieved,
                failing=failing
            )

        # Retrieving piece of a day for each event_type
        else:
            next_checkpoint, attempt = pull_recent_events(
                context=context,
                event_type=event_type,
                checkpoint=checkpoint,
                attempt=attempt,
                retrieved=retrieved,
                failing=failing
            )
            event_types[event_type]["attempt"] = attempt

        event_types[event_type]["checkpoint"] = next_checkpoint
        args["retrieved"] = retrieved
        args["failing"] = failing
        args["event_types"] = event_types

        return False
    return True


def orchestrator_function(context: df.DurableOrchestrationContext):
    args: dict = context.get_input()
    event_types: dict = args.get("event_types")
    interval: int = args.get("interval")

    logging.info(
        f"SingletonEternalOrchestrator: event_types: {list(event_types)} instance_id: {context.instance_id}"
    )

    if not event_types:
        return

    is_done = pull_history(context=context)
    if not is_done:
        context.continue_as_new(args)
        return

    is_done = pull_recent(context=context)
    if not is_done:
        context.continue_as_new(args)
        return

    retrieved_events_history = args.get("retrieved_history", "none")
    retrieved_events = args.get("retrieved", "none")
    failed_events = args.get("failing", "none")
    failed_history = args.get("failing_history", "none")
    logging.info(
        f"Fetch events finished. Retrieved History: {retrieved_events_history}, Retrieved Events: {retrieved_events}, Failed History: {failed_history}, Failed Events: {failed_events}"
    )
    args.pop("retrieved_history", None)
    args.pop("retrieved", None)
    args.pop("failing_history", None)
    args.pop("failing", None)

    for event_type_args in event_types.values():
        event_type_args["attempt"] = 0
    args["event_types"] = event_types

    # sleep
    logging.info(
        f"SingletonEternalOrchestrator: Sleeping for {interval} minutes")
    yield context.create_timer(
        context.current_utc_datetime + timedelta(minutes=interval)
    )
    logging.info(
        "SingletonEternalOrchestrator: Woke up and will continue as new.")
    context.continue_as_new(args)


main = df.Orchestrator.create(orchestrator_function)
