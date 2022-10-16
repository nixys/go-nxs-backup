package cmd

import appctx "github.com/nixys/nxs-go-appctx/v2"

func TestConfig(appCtx *appctx.AppContext) error {
	appCtx.Log().Info("The configuration is correct.")
	return nil
}
