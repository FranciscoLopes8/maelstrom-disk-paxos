#!/usr/bin/env python3

# This file contains an ongoing implementation of Disk Paxos.
# The basic ideas are here, but the code is still buggy and incomplete.
# There are known safety/liveness issues that require further work.
# Leaving this in the repository to come back to it when time permits.

import logging
from concurrent.futures import ThreadPoolExecutor
from ms import send, receiveAll, reply, exitOnError

logging.getLogger().setLevel(logging.DEBUG)
executor = ThreadPoolExecutor(max_workers=2)

node_id = None
node_ids = []

disk_block = {  # processor_id -> block
    "mbal": None,  # max promised ballot
    "bal": None,  # accepted ballot
    "input": None,  # accepted value
}

current_op = None

status = None

current_ballot_number = 0
current_ballot = (0, None)
highest_mbal_seen = (0, None)

pending_client = None
proposed_value = None
phase1_responses = []
write_completed = False

accept_acks = set()

pending_read = None
read_responses = []
read_completed = False


def handle_init(msg):
    global node_id, node_ids, disk_blocks, ROLE, DISKS, PROCESSORS

    nodes = sorted(node_ids)

    num_nodes = len(nodes)
    num_disks = num_nodes // 2 + 1

    DISKS = nodes[-num_disks:]
    PROCESSORS = nodes[:-num_disks]

    if node_id in DISKS:
        ROLE = "disk"
    else:
        ROLE = "processor"


def handle(msg):
    match msg.body.type:
        case "init":
            init_node(msg)
            handle_init(msg)
        case "write":
            handle_disk_write(msg)
        case "read":
            handle_client_read(msg)
        case "cas":
            reply(msg, type="error", code=10, text="Not Supported")
        case "mbal":
            handle_mbal_disks(msg)
        case "mbal_ok":
            handle_phase1_read(msg)
        case "mbal_reject":
            handle_mbal_reject(msg)
        case "accept":
            handle_accept(msg)
        case "accept_ok":
            handle_accept_ok(msg)
        case "accept_reject":
            handle_accept_reject(msg)
        case _:
            logging.warning("unknown message type %s", msg.body.type)


def init_node(msg):
    global node_id, node_ids
    node_id = msg.body.node_id
    node_ids = msg.body.node_ids
    reply(msg, type="init_ok")


def handle_disk_write(msg):
    global \
        current_ballot, \
        current_ballot_number, \
        proposed_value, \
        phase1_responses, \
        status, \
        pending_client, \
        current_op, \
        write_completed

    if ROLE == "disk":
        return

    pending_client = msg
    proposed_value = msg.body.value
    phase1_responses = []
    status = "Phase 1"
    current_op = "write"
    write_completed = False

    current_ballot_number += 1
    current_ballot = (current_ballot_number, node_id)

    for disk in DISKS:
        send(node_id, disk, type="mbal", mbal=current_ballot)


def handle_mbal_disks(msg):
    global node_id, disk_blocks, ROLE

    if ROLE == "processor":
        return

    block = disk_block

    incoming_mbal = msg.body.mbal

    if block["mbal"] is None or incoming_mbal > block["mbal"]:
        block["mbal"] = incoming_mbal

        reply(
            msg,
            type="mbal_ok",
            mbal=block["mbal"],
            bal=block["bal"],
            inp=block["input"],
        )
    else:
        reply(msg, type="mbal_reject", mbal=block["mbal"])


def handle_mbal_reject(msg):
    global \
        status, \
        phase1_responses, \
        current_ballot_number, \
        current_ballot, \
        pending_client, \
        write_completed, \
        read_completed, \
        highest_mbal_seen

    logging.debug("Received mbal_reject from %s", msg.src)
    phase1_responses = []
    status = None

    incoming_mbal = tuple(msg.body.mbal)
    highest_mbal_seen = max(highest_mbal_seen, incoming_mbal)

    if write_completed or read_completed:
        return

    if pending_client:
        logging.info("Retrying Phase 1 due to mbal_reject")
        current_ballot_number += 1
        current_ballot = (current_ballot_number, node_id)

        # Restart Phase 1
        for disk in DISKS:
            send(node_id, disk, type="mbal", mbal=current_ballot)

        status = "Phase 1"


def handle_phase1_read(msg):
    global \
        phase1_responses, \
        status, \
        proposed_value, \
        current_ballot, \
        pending_read, \
        DISKS, \
        ROLE, \
        current_op, \
        highest_mbal_seen

    if ROLE != "processor":
        return

    incoming_mbal = tuple(msg.body.mbal)
    highest_mbal_seen = max(highest_mbal_seen, incoming_mbal)

    if incoming_mbal < current_ballot:
        return

    incoming_bal = tuple(msg.body.bal) if msg.body.bal else None
    phase1_responses.append({"bal": incoming_bal, "inp": msg.body.inp})

    majority = len(DISKS) // 2 + 1
    if len(phase1_responses) < majority:
        return

    if status != "Phase 1":
        return

    accepted = [r for r in phase1_responses if r["inp"] is not None]

    if accepted:
        chosen = max(accepted, key=lambda r: r["bal"])
        proposed_value = chosen["inp"]

        phase1_responses = []
        status = None

    if current_op == "read":
        if pending_read is not None:
            finish_read_or_writeback()
    else:
        start_phase2()


def start_phase2():
    global node_id, DISKS, current_ballot, proposed_value, accept_acks, status

    status = "Phase 2"

    accept_acks = set()

    logging.debug(
        "Starting Phase 2: ballot=%s value=%s", current_ballot, proposed_value
    )

    for disk in DISKS:
        send(node_id, disk, type="accept", bal=current_ballot, inp=proposed_value)


def handle_accept(msg):
    global disk_blocks, ROLE

    if ROLE != "disk":
        return

    block = disk_block

    bal = msg.body.bal
    inp = msg.body.inp

    if block["mbal"] is None or bal >= block["mbal"]:
        block["mbal"] = bal
        block["bal"] = bal
        block["input"] = inp

        reply(msg, type="accept_ok", bal=bal)
    else:
        reply(msg, type="accept_reject", mbal=block["mbal"])


def handle_accept_ok(msg):
    global \
        accept_acks, \
        pending_client, \
        pending_read, \
        DISKS, \
        current_op, \
        status, \
        write_completed, \
        read_completed

    if ROLE != "processor":
        return

    if status != "Phase 2":
        return

    if write_completed or read_completed:
        return

    incoming_bal = tuple(msg.body.bal)

    if incoming_bal != current_ballot:
        return

    accept_acks.add(msg.src)

    majority = len(DISKS) // 2 + 1
    if len(accept_acks) < majority:
        return

    if current_op == "write" and pending_client:
        reply(pending_client, type="write_ok")
        pending_client = None

    if current_op == "read" and pending_read:
        reply(pending_read, type="read_ok", value=proposed_value)
        pending_read = None

    current_op = None
    status = None


def handle_accept_reject(msg):
    global \
        status, \
        current_ballot_number, \
        current_ballot, \
        proposed_value, \
        pending_client, \
        phase1_responses

    if ROLE != "processor":
        return

    logging.debug("Received accept_reject from %s", msg.src)

    # Reset Phase 1
    phase1_responses = []


def handle_client_read(msg):
    global \
        pending_read, \
        read_responses, \
        current_ballot_number, \
        current_ballot, \
        status, \
        current_op, \
        proposed_value, \
        read_completed

    if ROLE == "disk":
        return

    pending_read = msg
    read_responses = []
    current_op = "read"
    status = "Phase 1"
    proposed_value = None
    read_completed = False

    current_ballot_number = highest_mbal_seen[0] + 1
    current_ballot = (current_ballot_number, node_id)

    for disk in DISKS:
        send(node_id, disk, type="mbal", mbal=current_ballot)


def finish_read_or_writeback():
    global pending_read, proposed_value, current_op, status, read_completed

    if proposed_value is None:
        read_completed = True

        reply(pending_read, type="read_ok", value=None)

        pending_read = None
        current_op = None
        status = None
        return

    start_phase2()


# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())
