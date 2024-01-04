import logging
from datetime import timedelta

import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    args: dict = context.get_input()
    event_types: dict = args.get('event_types')
    interval: int = args.get('interval')

    logging.info(
        f'SingletonEternalOrchestrator: event_types: {event_types.keys()} instance_id: {context.instance_id}')

    if not event_types:
        return

    # Retrieving full days for each event_type one by one
    failing = args.get('failing', [])
    for event_type in list(event_types.keys()):
        event_type_args = event_types.get(event_type)
        if event_type in failing:
            continue

        attempt = event_type_args.pop('attempt', 0)
        remaining_days = event_type_args['days_to_collect']
        if remaining_days > 0 and attempt <= 3:
            try:
                yield context.call_activity('FetchAndSendByDayActivity', {'day': remaining_days, 'event_type': event_type})
                args[event_type]['days_to_collect'] = remaining_days-1
            except Exception as ex:
                args[event_type]['attempt'] = attempt + 1
                logging.error(str(ex))
                if attempt <= 3:
                    logging.info(f"Retrying attempt {attempt}.")
                else:
                    failing.add(event_type)
                    args['failing'] = failing

            # Run the orchastrator new for each day to help avoid timeouts.
            context.continue_as_new(args)
            return

    # Retrieving piece of a day for each event_type one by one

    retrieved = args.get('retrieved', [])
    for event_type in list(event_types.keys()):
        event_type_args = event_types.get(event_type)

        attempt = event_type_args.pop('attempt', 0)
        if event_type in failing or event_type in retrieved or attempt > 3:
            continue

        try:
            checkpoint = event_type_args['checkpoint']
            next_checkpoint = yield context.call_activity('FetchAndSendActivity',  {'checkpoint': checkpoint, 'event_type': event_type})
            args[event_type]['checkpoint'] = next_checkpoint
            retrieved.add(event_type)
            args['retrieved'] = retrieved
        except Exception as ex:
            args[event_type]['attempt'] = attempt + 1
            logging.error(str(ex))
            if attempt <= 3:
                logging.info(f"Retrying attempt {attempt}.")
            else:
                failing.add(event_type)
                args['failing'] = failing

        context.continue_as_new(args)

    args.pop('retrieved')
    args.pop('failing')
    # to consider, we could add some logs as a summary

    # sleep
    logging.info(
        f'SingletonEternalOrchestrator: Sleeping for {interval} minutes')
    yield context.create_timer(context.current_utc_datetime + timedelta(minutes=interval))
    logging.info(
        f'SingletonEternalOrchestrator: Woke up and will continue as new.')
    context.continue_as_new(args)


main = df.Orchestrator.create(orchestrator_function)
