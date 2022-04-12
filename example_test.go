package bayeux

import (
	"context"
	"fmt"
)

func Example() {
	ctx := context.Background()
	out := make(chan MaybeMsg)
	replay := "-1"
	b := Bayeux{}
	// Create a variable of type AuthenticationParameters and set the values
	var ap AuthenticationParameters
	ap.ClientID = "3MVG9pRsdbjsbdjfm1I.fz3f7zBuH4xdKCJcM9B5XLgxXh2AFTmQmr8JMn1vsadjsadjjsadakd_C"
	ap.ClientSecret = "E9FE118633BC7SGDADUHUE81F19C1D4529D09CB7231754AD2F2CA668400619"
	ap.Username = "salesforce.user@email.com"
	ap.Password = "foobar"
	ap.TokenURL = "https://login.salesforce.com/services/oauth2/token"
	creds, _ := GetSalesforceCredentials(ap)
	c := b.Channel(ctx, out, replay, *creds, "channel")
	for {
		select {
		case e := <-c:
			fmt.Printf("TriggerEvent Received: %+v", e)
		}
	}
}
