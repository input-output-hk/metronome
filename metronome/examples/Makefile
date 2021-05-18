PROJECT_DIR  := ../..
SCALA_VER    := 2.13.4
SCALA_SRC    := $(shell find $(PROJECT_DIR)/metronome -type f \( -wholename "*/src/*.scala" -o -wholename "*/resources/*.conf" \))
ASSEMBLY_JAR := $(PROJECT_DIR)/out/metronome/$(SCALA_VER)/examples/assembly/dest/out.jar

test:
	cd $(PROJECT_DIR) && mill metronome[$(SCALA_VER)].examples.test

$(ASSEMBLY_JAR): $(SCALA_SRC)
	cd $(PROJECT_DIR) && mill metronome[$(SCALA_VER)].examples.assembly

robot-%: $(ASSEMBLY_JAR)
	$(eval N = $*)
	java -cp $(ASSEMBLY_JAR) io.iohk.metronome.examples.robot.app.RobotApp --node-index $(N)
