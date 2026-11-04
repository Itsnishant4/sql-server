// @ts-check
import { module } from "@prisma/composer";
import serverService from "./service.mjs";

export default module("sql-server", ({ provision }) => {
  provision(serverService);
});
