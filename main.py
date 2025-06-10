import argparse

import forecast

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "location",
        help="Name of location in the configured coordinates."
    )
    args = parser.parse_args()
    responses = forecast.request_forecast(args.location)
    cct = forecast.concat(responses)
    message = forecast.extract_message(cct, args.location)

    print(message[:140])

    length = len(message)
    if length > 140:
        print(f"\n Truncated {message[140:]}")
    else:
        print(f"Message length: {length}")
