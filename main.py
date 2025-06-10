import forecast
import garmin


if __name__ == "__main__":
    lat, lon = garmin.get_inreach_position()
    loc = garmin.get_forecast_location(lat, lon)

    if loc:
        responses = forecast.request_forecast(loc)
        cct = forecast.concat(responses)
        message = forecast.extract_message(cct, loc)
        length = len(message)

        full_message = "\n".join([
            message[:140],
            f"Message length: {length}",
            f"Truncated: {message[140:]}"
        ])

        garmin.send_email("forecast test", full_message)
